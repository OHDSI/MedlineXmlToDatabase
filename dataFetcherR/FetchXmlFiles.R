# An R script for automatically downloading PubMed update files using HTTPS
# (for when your IT department has blocked FTP)
unprocessedFolder <- "D:/Medline/Unprocessed"
processedFolder <- "D:/Medline/2023"

# No changes below this line ---------------------------------------------------
baseUrl <- "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles"

processedFiles <- list.files(processedFolder, pattern = "*.xml.gz")

page <- readLines("https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/")
availableFiles <- unlist(stringr::str_extract_all(page, "pubmed[^.]+.xml.gz"))

toProcessFiles <- availableFiles[!availableFiles %in% processedFiles]

message(sprintf("Downloading %d files", length(toProcessFiles)))

pg <- txtProgressBar(min = 0, max = length(toProcessFiles), style = 3)
# fileName <- "pubmed23n1167.xml.gz"
for (i in seq_along(toProcessFiles)) {
  fileName <- toProcessFiles[i]
  if (!file.exists(file.path(unprocessedFolder, fileName))) {
    download.file(url = file.path(baseUrl, fileName),
                  destfile = file.path(unprocessedFolder, fileName),
                  method = "curl",
                  quiet = TRUE)
  }
  setTxtProgressBar(pg, i)
}
close(pg)

# Code for JANE ----------------------------------------------------------------
# Also downloading some files for JANE. Ignore
janeTempFolder <- "D:/Jane/temp"

download.file(url = "https://ftp.ncbi.nlm.nih.gov/pubmed/J_Medline.txt",
              destfile = file.path(janeTempFolder, "J_Medline.txt"),
              method = "curl")

download.file(url = "https://www.ncbi.nlm.nih.gov/pmc/front-page/jlist.csv",
              destfile = file.path(janeTempFolder, "jlist.csv"),
              method = "curl")

# Using -L curl argument to handle redirect:
download.file(url = "https://doaj.org/csv",
              destfile = file.path(janeTempFolder, "doaj.csv"),
              method = "curl",
              extra = "-L")

