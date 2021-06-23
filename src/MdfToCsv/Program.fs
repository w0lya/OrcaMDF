module MdfToCsv.Main

// TODO: error handling and logging

open System
open System.Globalization
open System.Diagnostics
open System.IO
open System.Text.RegularExpressions
open OrcaMDF.Core.Engine

let [<Literal>] logFileHeader = "FileName,DateProcessed,Status"

let remove x (xs : 'a array) =
    match Array.tryFindIndex ((=) x) xs with
    | Some 0 -> xs.[1..]
    | Some i -> Array.append xs.[..i-1] xs.[i+1..]
    | None   -> xs

let removeAt ind (xs: 'a array) =
    Array.append xs.[..ind-1] xs.[ind+1..]

let constructWideRows (config: Configuration.AppSettings) (tables: Map<string, (string[] * seq<string[]>)>) =
    let rec constructWideRowsRec mappingIndex accRows accColumns =
        if mappingIndex >= (config.TableMappings |> Array.length)
        then
            (accColumns |> String.concat ","),
            (accRows |> Seq.map (fun row ->  row |> String.concat ","))            
        else
            let mapping = config.TableMappings.[mappingIndex]
            let fk = mapping.LeftTableFK
            // Append right table data to the left table (or to the previous result)            
            let rightCols, rightRows = tables.[mapping.RightTableName]
            let leftFkIndex = Array.IndexOf(accColumns, fk)
            let rightFKIndex = Array.IndexOf(rightCols, fk)
           
            let merged = accRows
                         |> Seq.map (fun (lRow: string[]) ->
                             let dataToAppend =
                                rightRows
                                |> Seq.tryFind(fun r ->  r.[rightFKIndex] = lRow.[leftFkIndex])
                             // For non-existent data, fill in with emptiness to keep the output dimensions correct
                             let appendWithNonMatches =
                                 if dataToAppend.IsNone then (Array.zeroCreate rightCols.Length)
                                 else (dataToAppend.Value |> removeAt rightFKIndex)
                             let resultRow = Array.append lRow appendWithNonMatches                            
                             resultRow)
                         
            let mergedColumns = Array.append accColumns (rightCols |> removeAt rightFKIndex)
            constructWideRowsRec (mappingIndex + 1) merged mergedColumns
            
    let columns, rows = tables.[config.TableMetadata.[0].Name]
    constructWideRowsRec 0 rows columns

[<EntryPoint>]
let main argv =    
    let root = Configuration.appSettings
    let config = Configuration.getConfigurationData root   
    
    // Get all the files matching the pattern from src folder.
    let files = Directory.GetFiles(config.SourceFolder)
    let filesFiltered = files                        
                        |> Array.filter(fun f ->
                            let name = Path.GetFileName(f)
                            let m = Regex.Match(name, config.FileNameRegex)
                            m.Success)
    
    // Filter based on 'was processed' -- log file.
    // Log format: FileName, DateProcessed, Status.
    let logRows = File.ReadAllText(config.LogFilePath)
                      .Replace("\r\n", "\n")
                      .Split([| '\n' |])
                      |> List.ofArray
                      |> List.tail              // Skip header
    
    let filesProcessed = logRows                         
                         |> Seq.map (fun row ->
                             let values = row.Split(',')
                             if values.Length < 3 then None
                             else (values.[0], values.[1], values.[2]) |> Some)
                         |> Seq.choose id                          
                         |> Seq.filter(fun  (_, _, status) ->                            
                             status = "Success")
                         |> Seq.map(fun (fileName, _,_) -> fileName)
    
    let filesToProcess = filesFiltered
                         |> Array.filter (fun f ->
                            let name = Path.GetFileName(f)
                            (filesProcessed |> Seq.contains name |> not) || (filesProcessed |> Seq.isEmpty))
    
    let tableNamesToExport = config.TableMetadata |> Array.map (fun tm -> tm.Name)
        
    let results = 
        filesToProcess
        |> Array.map (fun file ->            
            let filePath = Path.GetFullPath(file)
            let db = new Database([filePath])
            let scanner = DataScanner(db)
           
            // Get data of interest based on config           
            let tables = db.Dmvs.Tables
                         |> List.ofSeq
                         |> Seq.filter(fun t ->
                             tableNamesToExport |> Array.contains t.Name)
                         |> Seq.map(fun t ->                             
                             let rows = scanner.ScanTable(t.Name)
                             let firstRow = rows |> Seq.head
                             let columnNames = firstRow.Columns                                               
                                               |> Seq.map (fun c -> c.Name)
                                               |> Seq.filter(fun c ->
                                                   let colsToSelect =
                                                      config.TableMetadata
                                                      |> Array.filter(fun x -> x.Name = t.Name)
                                                      |> Array.map(fun x -> x.Columns)
                                                      |> Array.concat
                                                   colsToSelect |> Array.contains c)
                                               |> Array.ofSeq
                             // Get data of interest
                             let dataRows = rows
                                            |> Seq.map (fun row ->
                                                columnNames
                                                |> Array.map(fun col ->
                                                    let value = row.[col]
                                                    if value = null then "NULL" else value.ToString()))                                            
                             (t.Name, (columnNames, dataRows)))
                         |> Map.ofSeq
            
            
            // 'Join' it in the code based on mappings
            let wideColumns, wideRows = constructWideRows config tables
           
            // Output a wide CSV            
            let fileName = Path.GetFileName(file).Replace(".mdf", ".csv")
            let destinationFilePath = sprintf "%s/%s"config.DestinationFolder fileName
            File.WriteAllLines(destinationFilePath, [wideColumns])
            File.AppendAllLines(destinationFilePath, wideRows)
            
            // Write to log
            if (logRows.Length = 0 || String.IsNullOrWhiteSpace(logRows.Head))
                then File.WriteAllLines(config.LogFilePath, [logFileHeader])
            
            let logLine = sprintf "%s,%s,%s"
                            filePath 
                            (TimeZoneInfo.ConvertTimeBySystemTimeZoneId(DateTime.UtcNow, "Pacific Standard Time")
                                .ToString("u", CultureInfo.CreateSpecificCulture("en-US")))
                            "Success"
            File.AppendAllLines(config.LogFilePath, [logLine])
            printfn "%A" DateTime.Now
            logLine)
    0
