using System;
using System.Collections;

namespace OrcaMDF.Core.Engine.SqlTypes
{
    public class SqlFloat: SqlTypeBase
    {
        private readonly byte precision;
		private readonly byte scale;

		public SqlFloat(byte precision, byte scale, CompressionContext compression)
			: base(compression)
		{
			this.precision = precision;
			this.scale = scale;
		}

		public override bool IsVariableLength
		{
			get { return CompressionContext.UsesVardecimals; }
		}

		public override short? FixedLength
		{
			// SQL Int = 4 bytes
			get { return Convert.ToInt16(getNumberOfRequiredStorageInts() * 4); }
		}

		private byte getNumberOfRequiredStorageInts()
		{
			// if (precision < 25)
			// 	return 1;
			//
			// if (precision <= 53)
			// 	return 2;

			return 2;
		}

		public override object GetValue(byte[] value)
		{
			if (value.Length != FixedLength.Value)
				throw new ArgumentException("Invalid value length: " + value.Length);

			var sqlFloat = new System.Data.SqlTypes.SqlSingle(BitConverter.ToDouble(value,0));

			return sqlFloat.Value;
		}
    }
}