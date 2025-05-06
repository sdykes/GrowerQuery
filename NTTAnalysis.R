library(tidyverse)

source("../CommonFunctions/getSQL.r")

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

BinDelivery2024 <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2024/BinDelivery.sql"))

GBD2024 <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2024/GraderBatchTeIpu.sql"))

OutputFromFieldBins <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2024/ma_Output_From_Field_Bins.sql"))

BinPresizeOutput <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2024/Bin_Presize_Output.sql"))

GraderBatchRepackProduction <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2024/ma_Grader_Batch_Repack_Production.sql"))

PoolDefintion <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2024/PoolDefinition.sql"))

DBI::dbDisconnect(con)


con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPacker2023Repl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

BinDelivery2023 <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2023/BinDelivery2023.sql"))

GBD2023 <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2023/GBD2023.sql"))

RTEsByGB2023 <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2023/RTEs2023.sql"))

DBI::dbDisconnect(con)


#
# NTT Bins delivered
#


NTTConsignments2024 <- BinDelivery |>
  filter(Season == 2024,
         Owner == "Ngai Tukairangi Trust") 

NTTConsignments2023 <- BinDelivery2023 |>
  filter(Season == 2023,
         Owner == "Ngai Tukairangi Trust") |>
  mutate(Orchard = if_else(Orchard == "NRL", "Te Wairua", Orchard))

NTTConsignments <- NTTConsignments2023 |>
  bind_rows(NTTConsignments2024)

write_csv(NTTConsignments, "NTTConsignments.csv")
  
#
# Summarise by production site and RPIN
#

NTTConsignmentSummary2024PS <- NTTConsignments2024 |>
  group_by(RPIN,Orchard,`Production site`) |>
  summarise(NoOfBins = sum(NoOfBins, na.rm=T),
            .groups = "drop") |>
  mutate(Season = 2024)

NTTConsignmentSummary2023PS <- NTTConsignments2023 |>
  group_by(RPIN,Orchard,`Production site`) |>
  summarise(NoOfBins = sum(NoOfBins, na.rm=T),
            .groups = "drop") |>
  mutate(Season = 2023)
  
NTTConsignmentSummary2024RPIN <- NTTConsignments2024 |>
  group_by(RPIN,Orchard) |>
  summarise(NoOfBins = sum(NoOfBins, na.rm=T),
            .groups = "drop") |>
  mutate(Season = 2024)

NTTConsignmentSummary2023RPIN <- NTTConsignments2023 |>
  group_by(RPIN,Orchard) |>
  summarise(NoOfBins = sum(NoOfBins, na.rm=T),
            .groups = "drop") |>
  mutate(Season = 2023)

#
# Aggregating Consignments
#

NTTConsignmentSummaryPS <- NTTConsignmentSummary2023PS |>
  bind_rows(NTTConsignmentSummary2024PS) |>
  pivot_wider(id_cols = c(RPIN, Orchard, `Production site`),
              names_from = Season,
              values_from = NoOfBins,
              values_fill = 0)

write_csv(NTTConsignmentSummaryPS, "NTTConsignmentSummaryPS.csv")
  
NTTConsignmentSummaryRPIN <- NTTConsignmentSummary2023RPIN |>
  bind_rows(NTTConsignmentSummary2024RPIN) |>
  pivot_wider(id_cols = c(RPIN, Orchard),
              names_from = Season,
              values_from = NoOfBins,
              values_fill = 0)

write_csv(NTTConsignmentSummaryRPIN, "NTTConsignmentSummaryRPIN.csv")

#
# RTE Analysis
#
# Look at 2024 (more complicated)
#

ExportBins <- OutputFromFieldBins |>
  filter(SeasonID == 2011) |>
  left_join(PoolDefintion |>
              select(-c(ProductDesc)), 
            by = "ProductID") |>
  group_by(GraderBatchID, PoolDesc) |>
  summarise(RTEs = sum(RTEs, na.rm=T),
            Kgs = sum(Kgs, na.rm=T),
            .groups = "drop") |>
  mutate(Source = "ExportBins") 

RepackBins <- GraderBatchRepackProduction |>
  filter(SeasonID == 2011) |>
  left_join(PoolDefintion |>
              select(-c(ProductDesc)), 
            by = "ProductID") |>
  group_by(GraderBatchID, PoolDesc) |>
  summarise(RTEs = sum(RTEs, na.rm=T),
            Kgs = sum(Kgs, na.rm=T),
            .groups = "drop") |>
  mutate(Source = "RepackBins") 

PresizeBins <- BinPresizeOutput |>
  filter(SeasonID == 2011) |>
  left_join(PoolDefintion |>
              select(-c(ProductDesc)), 
            by = c("PresizeProductID" = "ProductID")) |>
  group_by(GraderBatchID, PoolDesc) |>
  summarise(RTEs = sum(RTEs, na.rm=T),
            Kgs = sum(Kgs, na.rm=T),
            .groups = "drop") |>
  mutate(Source = "PresizeBins") 

RTEsByPool <- ExportBins |>
  bind_rows(RepackBins) |>
  bind_rows(PresizeBins) |>
  group_by(GraderBatchID, PoolDesc) |>
  summarise(RTEs = sum(RTEs),
            Kgs = sum(Kgs),
            .groups = "drop") |>
  mutate(PoolTemp = str_sub(PoolDesc, 15,-1),
         PoolTemp = as.character(PoolTemp),
         Pool = case_when(PoolTemp %in% c("0","145","160") ~ "Bulk",
                          TRUE ~ PoolTemp)) |>
  select(-c(PoolDesc, PoolTemp)) |>
  group_by(GraderBatchID, Pool) |>
  summarise(RTEs = sum(RTEs),
            .groups = "drop") |>
  pivot_wider(id_cols = GraderBatchID,
              names_from = Pool,
              values_from = RTEs,
              values_fill = 0) |>
  rowwise() |>
  mutate(TotalRTEs = sum(c_across(c(`53`:Bulk))))

#
# GraderBatchSummaries
#

NTTBatches2024 <- GBD2024 |>
  filter(Season == 2024,
         !PresizeInputFlag,
         Owner == "Ngai Tukairangi Trust") |>
  mutate(Packout = 1-RejectKgs/InputKgs) |>
  left_join(RTEsByPool |> 
              select(c(GraderBatchID, TotalRTEs)),
            by = "GraderBatchID")

NTTBatches2023 <- GBD2023 |>
  filter(Season == 2023,
         !PresizeInputFlag,
         Owner == "Ngai Tukairangi Trust") |>
  mutate(Packout = 1-RejectKgs/InputKgs,
         Orchard = if_else(Orchard == "NRL", "Te Wairua", Orchard)) |>
  left_join(RTEsByGB2023 |> 
              select(c(GraderBatchID, TotalRTEs)),
            by = "GraderBatchID")

NTTBatches <- NTTBatches2023 |>
  bind_rows(NTTBatches2024)

write_csv(NTTBatches, "NTTBatches.csv")

#
# Summarise by PS and RPIN
#

NTTBatchSummary2024PS <- NTTBatches2024 |>
  group_by(RPIN,Orchard,`Production site`) |>
  summarise(FieldBinsTipped = sum(FieldBinsTipped, na.rm=T),
            InputKgs = sum(InputKgs, na.rm=T),
            RejectKgs = sum(RejectKgs, na.rm=T),
            TotalRTEs = sum(TotalRTEs, na.rm=T),
            .groups = "drop") |>
  mutate(Packout = 1-RejectKgs/InputKgs,
         Season = 2024)

NTTBatchSummary2023PS <- NTTBatches2023 |>
  group_by(RPIN,Orchard,`Production site`) |>
  summarise(FieldBinsTipped = sum(FieldBinsTipped, na.rm=T),
            InputKgs = sum(InputKgs, na.rm=T),
            RejectKgs = sum(RejectKgs, na.rm=T),
            TotalRTEs = sum(TotalRTEs, na.rm=T),
            .groups = "drop") |>
  mutate(Packout = 1-RejectKgs/InputKgs,
         Season = 2023)

NTTBatchSummary2024RPIN <- NTTBatches2024 |>
  group_by(RPIN,Orchard) |>
  summarise(FieldBinsTipped = sum(FieldBinsTipped, na.rm=T),
            InputKgs = sum(InputKgs, na.rm=T),
            RejectKgs = sum(RejectKgs, na.rm=T),
            TotalRTEs = sum(TotalRTEs, na.rm=T),
            .groups = "drop") |>
  mutate(Packout = 1-RejectKgs/InputKgs,
         Season = 2024)

NTTBatchSummary2023RPIN <- NTTBatches2023 |>
  group_by(RPIN,Orchard) |>
  summarise(FieldBinsTipped = sum(FieldBinsTipped, na.rm=T),
            InputKgs = sum(InputKgs, na.rm=T),
            RejectKgs = sum(RejectKgs, na.rm=T),
            TotalRTEs = sum(TotalRTEs, na.rm=T),
            .groups = "drop") |>
  mutate(Packout = 1-RejectKgs/InputKgs,
         Season = 2023)

#
# Aggregate the season together
#

NTTBatchSummaryPS <- NTTBatchSummary2023PS |>
  bind_rows(NTTBatchSummary2024PS)

write_csv(NTTBatchSummaryPS,"NTTBatchSummaryPS.csv")

NTTBatchSummaryRPIN <- NTTBatchSummary2023RPIN |>
  bind_rows(NTTBatchSummary2024RPIN)

write_csv(NTTBatchSummaryRPIN,"NTTBatchSummaryRPIN.csv")




