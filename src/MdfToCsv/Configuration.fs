module MdfToCsv.Configuration

open Microsoft.Extensions.Configuration
open System.IO
open System

[<CLIMutable>]
type TableMetadata =
    {
        Name: string
        Columns: string []
    }

[<CLIMutable>]
type TableMappings =
    {
        LeftTableName: string
        RightTableName: string
        RightTablePK: string
        LeftTableFK: string
    }

[<CLIMutable>]
type AppSettings =
    {
        SourceFolder: string
        DestinationFolder: string
        LogFilePath: string
        FileNameRegex: string
        TableMetadata: TableMetadata []
        TableMappings: TableMappings []
    }

let appSettings = 
    ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional = true)
        .Build()
 
let internal validateSettings config =
    let validationErrors =
        [ if String.IsNullOrWhiteSpace(config.SourceFolder) then
            "ERROR: SourceFolder not specified in appsettings."
          if String.IsNullOrWhiteSpace(config.DestinationFolder) then
            "ERROR: DestinationFolder not specified in appsettings."
          if String.IsNullOrWhiteSpace(config.FileNameRegex) then
            "ERROR: File name filter (FileNameRegex) not specified in appsettings."
          if String.IsNullOrWhiteSpace(config.LogFilePath) then
            "ERROR: Log file path (LogFilePath) not specified in appsettings."
          if config.TableMetadata.Length = 0 then
            "ERROR: no data to extract (TableMetadata) specified."
          if (config.TableMappings.Length <> config.TableMetadata.Length - 1) then
            "ERROR: missing or redundant table relationships specified."
           ]
    if not validationErrors.IsEmpty then
        validationErrors |> String.concat "\n" |> failwith
        
let getConfigurationData (appSettings: IConfigurationRoot) =    
    let settings = appSettings.GetSection("AppSettings").Get<AppSettings>()
    printfn "%A" settings
    validateSettings settings
    settings