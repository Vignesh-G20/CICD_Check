[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "2021 (1)_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"2021 (1)" = let
  Source = Excel.Workbook(Web.Contents("https://xencia1-my.sharepoint.com/personal/vignesh_g_xencia_com/Documents/Apps/Microsoft Power Query/Uploaded Files/2021 (1).xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "2021 (1)", Kind = "Sheet"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Column1", type text}, {"Column2", Int64.Type}, {"Column3", type date}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", Int64.Type}, {"Column8", type number}, {"Column9", type number}}, "en"),
  #"Choose columns" = Table.SelectColumns(#"Changed column type", {"Column1", "Column2", "Column6", "Column7", "Column8", "Column9"}),
  #"Split column by delimiter" = Table.SplitColumn(#"Choose columns", "Column6", Splitter.SplitTextByDelimiter("-"), {"Column6.1", "Column6.2", "Column6.3"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Split column by delimiter", {{"Column6.1", type text}, {"Column6.2", type text}, {"Column6.3", type text}}, "en"),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 1", {{"Column1", "SNo"}}),
  #"Cleaned text" = Table.TransformColumns(#"Renamed columns", {{"Column6.1", each Text.Clean(_), type nullable text}}),
  #"Cleaned text 1" = Table.TransformColumns(#"Cleaned text", {{"Column6.3", each Text.Clean(_), type nullable text}}),
  #"Replaced value" = Table.ReplaceValue(#"Cleaned text 1", "Road", "  Road", Replacer.ReplaceText, {"Column6.1"}),
  #"Cleaned text 2" = Table.TransformColumns(#"Replaced value", {{"Column6.1", each Text.Clean(_), type nullable text}}),
  #"Trimmed text" = Table.TransformColumns(#"Cleaned text 2", {{"Column6.1", each Text.Trim(_), type nullable text}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Trimmed text", "Road", "!#Road)", Replacer.ReplaceText, {"Column6.1"}),
  #"Cleaned text 3" = Table.TransformColumns(#"Replaced value 1", {{"Column6.1", each Text.Clean(_), type nullable text}})
in
  #"Cleaned text 3";
shared #"2021 (2)" = let
  Source = Excel.Workbook(Web.Contents("https://xencia1-my.sharepoint.com/personal/vignesh_g_xencia_com/Documents/Apps/Microsoft Power Query/Uploaded Files/2021 (1).xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "2021 (1)", Kind = "Sheet"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Column1", type text}, {"Column2", Int64.Type}, {"Column3", type date}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", Int64.Type}, {"Column8", type number}, {"Column9", type number}}, "en")
in
  #"Changed column type";
shared #"2021 (3)" = let
  Source = #"2021 (2)"
in
  Source;
shared #"2021 (1)_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "883ac136-9d9d-429c-a3d4-50dd3e3df5f6"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "a4d2f381-f601-448f-b586-56bde2a6db83"]}[Data],
  TableNavigation = Navigation_2{[Id = "Test_Data", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
