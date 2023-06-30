#' Data storage class with SQL Server provider
#'
#' @description
#' Implementation of the DataStorage R6 class to SQL Server backend using a
#' unified API for read/write operations
#'
#' @export
#'
#' @examples
#' \dontrun{
#' data_storage <- DataStorageSqlServer$new(user = "myuser", password = "mysecretpassword")
#'
#' data_storage$insert("example", "test_event", "session1")
#' data_storage$insert("example", "input", "s1", list(id = "id1"))
#' data_storage$insert("example", "input", "s1", list(id = "id2", value = 32))
#'
#' data_storage$insert(
#'   "example", "test_event_3_days_ago", "session1",
#'   time = lubridate::as_datetime(lubridate::today() - 3)
#' )
#'
#' data_storage$read_event_data()
#' data_storage$read_event_data(Sys.Date() - 1, Sys.Date() + 1)
#' data_storage$close()
#' }
DataStorageSqlServer <- R6::R6Class( # nolint object_name_linter
  classname = "DataStorageSqlServer",
  inherit = DataStorageSQLFamily,
  #
  # Public
  public = list(

    #' @description
    #' Initialize the data storage class
    #' @param driver string with an ODBC driver name for SQL Server.
    #' @param username string with a SQL Server username.
    #' @param password string with the password for the username.
    #' @param hostname string with hostname of SQL Server instance.
    #' @param dbname string with the name of the database in the SQL Server
    #' instance.

    initialize = function(
      driver = "SQL Server",
      username = NULL,
      password = NULL,
      hostname = "127.0.0.1",
      dbname = "shiny_telemetry"
    ) {
      checkmate::assert_string(driver)
      checkmate::assert_string(username)
      checkmate::assert_string(password)
      checkmate::assert_string(hostname)
      checkmate::assert_string(dbname)

      logger::log_debug(
        "Parameters for SQL Server:\n",
        "  *            driver: {driver}\n",
        "  *          username: {username}\n",
        "  * password (sha256): {digest::digest(password, algo = 'sha256')}\n",
        "  *          hostname: {hostname}\n",
        "  *           db name: {dbname}\n",
        namespace = "shiny.telemetry"
      )
      private$connect(driver, username, password, hostname, dbname)
      private$initialize_connection()
    }

  ),
  #
  # Private
  private = list(
    # Private Fields
    db_con = NULL,

    # Private methods

    initialize_connection = function() {
      table_schemes <- list(
        c(
          time = "datetime",
          app_name = "nvarchar(1024)",
          session = "nvarchar(1024)",
          type = "nvarchar(1024)",
          details = "nvarchar(max)"
        )
      )

      table_names <- c(self$event_bucket)
      names(table_schemes) <- table_names

      purrr::walk2(
        table_names, table_schemes, private$create_table_from_schema
      )
      NULL
    },

    connect = function(driver, user, password, hostname, dbname) {
      # Initialize connection with database
      private$db_con <- odbc::dbConnect(
        odbc::odbc(),
        driver = driver,
        UID = user,
        PWD = password,
        database = dbname,
        server = hostname
      )
    },

    read_data = function(date_from, date_to, bucket) {
      checkmate::assert_choice(bucket, c(self$event_bucket))

      query <- private$build_query_sql(
        bucket, date_from, date_to
      )

      odbc::dbGetQuery(private$db_con, query) %>%
        dplyr::tibble() %>%
        private$unnest_json("details")

    },

    build_query_sql = function(
      bucket, date_from = NULL, date_to = NULL
    ) {
      checkmate::assert_date(date_from, null.ok = TRUE)
      checkmate::assert_date(date_to, null.ok = TRUE)

      query <- list(
        .sep = " ",
        "SELECT *",
        "FROM {bucket}",
        ifelse(!is.null(date_from) || !is.null(date_to), "WHERE", "")
      )

      build_timestamp <- function(value) {  # nolint: object_usage_linter
        seconds <- lubridate::as_datetime(value) %>% as.double()
        glue::glue("DATEADD(s, {seconds}, '1970-01-01')")
      }

      where <- list(.sep = " AND ")
      if (!is.null(date_from)) {
        where <- c(
          where,
          glue::glue(
            "time >= ",
            "{build_timestamp(date_from)}"
          )
        )
      }

      if (!is.null(date_to)) {
        date_to_aux <- (lubridate::as_date(date_to) + 1) %>% # nolint: object_usage_linter
          lubridate::as_datetime()
        where <- c(
          where,
          glue::glue("time < {build_timestamp(date_to_aux)}")
        )
      }

      query <- c(query, do.call(glue::glue, where))
      do.call(glue::glue, query) %>% stringr::str_trim()
    }
  )
)
