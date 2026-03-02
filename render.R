output_dir <- "site"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

rmarkdown::render(
  input = "quebrasnaturais_SIA.Rmd",
  output_format = "html_document",
  output_file = "index.html",
  output_dir = output_dir,
  quiet = TRUE
)

