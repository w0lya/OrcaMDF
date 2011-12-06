using System;
using System.Collections.Generic;
using System.Linq;
using OrcaMDF.Core.Engine.Pages.PFS;
using OrcaMDF.Core.MetaData;
using OrcaMDF.Core.MetaData.Enumerations;

namespace OrcaMDF.Core.Engine
{
	public class DataScanner : Scanner
	{
		public DataScanner(Database database)
			: base(database)
		{ }

		/// <summary>
		/// Will scan any table - heap or clustered - and return an IEnumerable of generic rows with data & schema
		/// </summary>
		public IEnumerable<Row> ScanTable(string tableName)
		{
			var schema = MetaData.GetEmptyDataRow(tableName);

			return scanTable(tableName, schema);
		}

		/// <summary>
		/// Will scan any table - heap or clustered - and return an IEnumerable of typed rows with data & schema
		/// </summary>
		internal IEnumerable<TDataRow> ScanTable<TDataRow>(string tableName) where TDataRow : Row, new()
		{
			var schema = new TDataRow();

			return scanTable(tableName, schema).Cast<TDataRow>();
		}

		/// <summary>
		/// Scans a linked list of pages returning an IEnumerable of typed rows with data & schema
		/// </summary>
		internal IEnumerable<TDataRow> ScanLinkedDataPages<TDataRow>(PagePointer loc, CompressionLevel compressionLevel) where TDataRow : Row, new()
		{
			return ScanLinkedDataPages(loc, new TDataRow(), compressionLevel).Cast<TDataRow>();
		}

		/// <summary>
		/// Starts at the data page (loc) and follows the NextPage pointer chain till the end.
		/// </summary>
		internal IEnumerable<Row> ScanLinkedDataPages(PagePointer loc, Row schema, CompressionLevel compressionLevel)
		{
			while (loc != PagePointer.Zero)
			{
				var page = Database.GetDataPage(loc);

				foreach (var dr in page.GetEntities(schema))
					yield return dr;

				loc = page.Header.NextPage;
			}
		}

		private IEnumerable<Row> scanTable(string tableName, Row schema)
		{
			// Get object
			var tableObject = Database.BaseTables.sysschobjs
				.Where(x => x.name == tableName)
				.Where(x => x.type.Trim() == ObjectType.INTERNAL_TABLE || x.type.Trim() == ObjectType.SYSTEM_TABLE || x.type.Trim() == ObjectType.USER_TABLE)
				.SingleOrDefault();
			if (tableObject == null)
				throw new ArgumentException("Table does not exist.");

			// Get rowset, prefer clustered index if exists
			var partitions = Database.Dmvs.Partitions
				.Where(x => x.ObjectID == tableObject.id && x.IndexID <= 1)
				.OrderByDescending(x => x.IndexID)
				.ThenBy(x => x.PartitionNumber);

			if (partitions.Count() == 0)
				throw new ArgumentException("Table has no partitions.");

			// Loop all partitions and return results one by one
			return partitions.SelectMany(partition => scanPartition(partition.PartitionID, partition.PartitionNumber, schema));
		}

		private IEnumerable<Row> scanPartition(long partitionID, int partitionNumber, Row schema)
		{
			// Lookup partition
			var partition = Database.Dmvs.Partitions
				.Where(p => p.PartitionID == partitionID && p.PartitionNumber == partitionNumber)
				.SingleOrDefault();

			if(partition == null)
				throw new ArgumentException("Partition (" + partitionID + "." + partitionNumber + " does not exist.");

			// Get allocation unit for in-row data
			var au = Database.Dmvs.SystemInternalsAllocationUnits
				.Where(x => x.ContainerID == partition.PartitionID && x.Type == 1)
				.SingleOrDefault();

			if (au == null)
				throw new ArgumentException("Partition (" + partition.PartitionID + "." + partition.PartitionNumber + " has no HOBT allocation unit.");

			// Before we can scan either heaps or indices, we need to know the compression level as that's set at the partition level, and not at the record/page level
			var compressionLevel = (CompressionLevel)partition.DataCompression;

			// Heap tables won't have root pages, thus we can check whether a root page is defined for the HOBT allocation unit
			if(au.RootPage != PagePointer.Zero)
			{
				// Index
				foreach (var row in ScanLinkedDataPages(au.FirstPage, schema, compressionLevel))
					yield return row;
			}
			else
			{
				// Heap
				foreach (var row in scanHeap(au.FirstIamPage, schema, compressionLevel))
					yield return row;
			}
		}

		/// <summary>
		/// Scans a heap beginning from the provided IAM page and onwards.
		/// </summary>
		private IEnumerable<Row> scanHeap(PagePointer loc, Row schema, CompressionLevel compressionLevel)
		{
			// Traverse the linked list of IAM pages untill the tail pointer is zero
			while (loc != PagePointer.Zero)
			{
				var iamPage = Database.GetIamPage(loc);

				// Create an array with all of the header slot pointers
				var iamPageSlots = new []
					{
						iamPage.Slot0,
						iamPage.Slot1,
						iamPage.Slot2,
						iamPage.Slot3,
						iamPage.Slot4,
						iamPage.Slot5,
						iamPage.Slot6,
						iamPage.Slot7
					};

				// Loop each header slot and yield the results, provided the header slot is allocated
				foreach (var slot in iamPageSlots.Where(x => x != PagePointer.Zero))
				{
					var dataPage = Database.GetDataPage(slot);

					foreach (var dr in dataPage.GetEntities(schema))
						yield return dr;
				}

				// Then loop through allocated extents and yield results
				foreach (var extent in iamPage.GetAllocatedExtents())
				{
					// Get PFS page that tracks this extent
					var pfs = Database.GetPfsPage(PfsPage.GetPfsPointerForPage(extent.StartPage));
					
					foreach (var pageLoc in extent.GetPagePointers())
					{
						// Check if page is allocated according to PFS page
						var pfsDescription = pfs.GetPageDescription(pageLoc.PageID);

						if(!pfsDescription.IsAllocated)
							continue;

						var dataPage = Database.GetDataPage(pageLoc);

						foreach (var dr in dataPage.GetEntities(schema))
							yield return dr;
					}
				}

				// Update current IAM chain location to the tail pointer
				loc = iamPage.Header.NextPage;
			}
		}
	}
}