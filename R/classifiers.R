# classifiers.R
# Functions for loading and managing classification lookups
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

# 1. Load all classifiers -----

#' Load classification lookups
#'
#' Loads lookup tables for contract types, professions, and economic sectors.
#' These classifiers provide human-readable labels for coded values in the
#' employment data.
#'
#' @param use_mock Logical. If TRUE, creates simplified mock classifiers for
#'   testing purposes. If FALSE, attempts to load from external files. Default
#'   is FALSE.
#'
#' @return A list with three elements:
#'   \itemize{
#'     \item professioni: Data.table mapping profession codes to labels
#'     \item etichette_contratti: Data.table mapping contract type codes to descriptions
#'     \item settori: Data.table mapping ATECO 3-digit codes to sector names
#'   }
#'
#' @details
#' The function loads three types of classification data:
#'
#' 1. **Profession codes**: Mapping of qualifica codes to profession descriptions
#' 2. **Contract types**: Harmonized contract type codes with Italian descriptions
#'    - Applies standardization to consolidate similar contract types
#'    - Maps obsolete codes to current equivalents
#' 3. **ATECO sectors**: 3-digit economic sector classifications (XX.Y format)
#'
#' When use_mock = TRUE, the function generates simplified lookup tables suitable
#' for testing without requiring external data files.
#'
#' @examples
#' \dontrun{
#' # Load real classifiers from files
#' lcla <- load_classifiers()
#'
#' # Load mock classifiers for testing
#' lcla_test <- load_classifiers(use_mock = TRUE)
#' }
#'
#' @export
load_classifiers <- function(use_mock = FALSE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required but not installed")
  }

  cla <- list()

  if (use_mock) {
    cat("Creating mock classifiers for testing...\n")

    # Mock profession codes (CP2011 format matching actual data)
    cla$professioni <- data.table::data.table(
      qualifica = c(
        "7.2.8", "7.1.5", "2.6.5", "2.6.4", "3.3.3", "8.1.4", "8.1.3", "8.4.3",
        "7.1.8", "7.1.7", "4.3.1", "4.2.2", "7.2.1", "5.2.2", "6.4.1", "7.2.7",
        "5.1.2", "5.5.2", "3.2.1", "8.2.2", "1.2.2", "2.5.1", "3.1.1", "4.1.1",
        "5.3.1", "6.1.3", "6.2.2", "7.3.1", "8.3.1"
      ),
      descrizione = c(
        "Commessi vendita ingrosso", "Addetti mensa ristorazione", "Tecnici costruzioni",
        "Tecnici industria", "Contabili amministrativi", "Operatori macchine ufficio",
        "Operatori inserimento dati", "Conducenti veicoli", "Addetti preparazione pasti",
        "Addetti servizi ristorazione", "Tecnici vendita", "Rappresentanti commercio",
        "Addetti vendita dettaglio", "Addetti servizi sicurezza", "Addetti pulizie uffici",
        "Commessi negozi", "Tecnici informatici", "Operai edilizia", "Impiegati amministrativi",
        "Operai magazzino", "Dirigenti amministrazione", "Specialisti gestione",
        "Professioni intellettuali", "Tecnici specializzati", "Operatori servizi",
        "Artigiani", "Operai industria", "Addetti servizi personali", "Operai trasporti"
      )
    )

    # Mock contract type labels
    cla$etichette_contratti <- data.table::data.table(
      COD_TIPOLOGIA_CONTRATTUALE = c(
        "A.01.00", "A.02.00", "A.03.08", "A.03.09",
        "B.03.00", "C.01.00", "G.03.00"
      ),
      DES_TIPOCONTRATTI = c(
        "Tempo Indeterminato",
        "Tempo Determinato",
        "Apprendistato Professionalizzante",
        "Apprendistato Qualifica",
        "Lavoro Intermittente",
        "Collaborazione Coordinata Continuativa",
        "Tirocinio"
      )
    )

    # Mock ATECO 3-digit sector labels (matching actual data format)
    cla$settori <- data.table::data.table(
      ateco = c(
        "82.9", "22.2", "85.2", "85.3", "73.1", "81.2", "10.5", "24.5", "20.4",
        "20.5", "27.3", "52.1", "52.2", "47.7", "17.2", "56.3", "46.3", "46.6",
        "25.6", "85.5", "01.1", "01.6", "10.1", "10.8", "13.1", "14.1", "15.1",
        "16.1", "18.1", "23.1", "28.1", "29.1", "30.1", "31.0", "32.1", "33.1",
        "35.1", "36.0", "37.0", "38.1", "39.0", "41.2", "42.1", "43.1", "45.1",
        "46.1", "46.2", "47.1", "47.2", "49.1", "49.3", "50.1", "51.1", "53.1",
        "55.1", "56.1", "58.1", "59.1", "60.1", "61.1", "62.0", "63.1", "64.1",
        "65.1", "66.1", "68.1", "69.1", "70.1", "71.1", "72.1", "74.2", "74.1",
        "75.0", "77.1", "78.1", "79.1", "80.1", "81.1", "82.1", "84.1", "85.1",
        "86.1", "87.1", "88.1", "90.0", "91.0", "92.0", "93.1", "94.1", "95.1",
        "96.0"
      ),
      nome = c(
        "Servizi supporto imprese", "Fabbricazione plastica", "Istruzione primaria",
        "Istruzione secondaria", "Pubblicità ricerche mercato", "Pulizia edifici",
        "Lavorazione conservazione carne", "Fabbricazione articoli metallo",
        "Fabbricazione fibre artificiali", "Fabbricazione pesticidi",
        "Fabbricazione cavi elettrici", "Magazzinaggio custodia", "Servizi trasporto",
        "Commercio dettaglio non specializzato", "Fabbricazione carta cartone",
        "Ristoranti servizi ristorazione mobile", "Commercio ingrosso materie prime",
        "Commercio ingrosso prodotti chimici", "Trattamento rivestimento metalli",
        "Altri servizi istruzione", "Coltivazioni agricole", "Attività di supporto",
        "Lavorazione conservazione carni", "Produzione prodotti panetteria",
        "Preparazione filatura fibre tessili", "Confezione articoli abbigliamento",
        "Confezione articoli pelle", "Taglio modellatura legno", "Stampa",
        "Fabbricazione vetro articoli vetro", "Fabbricazione macchine uso generale",
        "Fabbricazione autoveicoli", "Fabbricazione navi imbarcazioni",
        "Fabbricazione mobili", "Fabbricazione gioielleria",
        "Riparazione manutenzione macchine", "Fornitura energia elettrica",
        "Raccolta trattamento distribuzione acqua", "Gestione reti fognarie",
        "Attività raccolta rifiuti", "Attività risanamento bonifiche",
        "Costruzione edifici", "Ingegneria civile", "Lavori costruzione specializzati",
        "Commercio riparazione autoveicoli", "Commercio ingrosso prodotti vari",
        "Commercio ingrosso alimentari", "Commercio dettaglio grandi magazzini",
        "Commercio dettaglio alimentari", "Trasporto terrestre passeggeri",
        "Trasporto terrestre merci", "Trasporto marittimo costiero",
        "Trasporto aereo passeggeri", "Servizi postali corriere", "Alloggio", "Ristoranti",
        "Attività editoriali", "Produzione cinematografica video",
        "Trasmissioni radiofoniche televisive", "Telecomunicazioni",
        "Programmazione consulenza informatica", "Servizi informazione",
        "Attività servizi finanziari", "Assicurazioni", "Fondi pensione",
        "Attività immobiliari", "Attività legali contabilità",
        "Attività direzione consulenza gestionale", "Ricerca scientifica",
        "Altre attività professionali scientifiche", "Studi design fotografici",
        "Attività veterinarie", "Noleggio", "Attività agenzie viaggio",
        "Attività servizi vigilanza investigazione", "Attività servizi edifici",
        "Attività pulizia", "Attività supporto uffici",
        "Amministrazione pubblica difesa", "Istruzione",
        "Assistenza sanitaria", "Assistenza sociale residenziale",
        "Assistenza sociale non residenziale", "Attività creative artistiche",
        "Biblioteche archivi musei", "Attività gioco scommesse",
        "Attività sportive intrattenimento", "Attività organizzazioni associative",
        "Riparazione computer beni personali", "Altre attività servizi persona"
      )
    )

    cat("  Created mock classifiers with limited entries\n")

  } else {
    # Load real classifiers from external files
    cat("Loading classifiers from external files...\n")

    # 1. Load profession codes
    prof_path <- "../../progetti/datasets/codici_p.rds"
    if (file.exists(prof_path)) {
      cla$professioni <- readRDS(prof_path)
      cat("  Loaded", nrow(cla$professioni), "profession codes\n")
    } else {
      warning("Profession codes file not found at: ", prof_path)
      cla$professioni <- data.table::data.table(
        qualifica = character(0),
        descrizione = character(0)
      )
    }

    # 2. Load and harmonize contract types
    contract_path <- "../../progetti/datasets/Co_standards/CO.Allegati.Decreto.Gennaio.2024/Rev.087 - Allegati al DD Gennaio.2024/Rev.087-ST-Classificazioni-Standard.xls"

    if (file.exists(contract_path)) {
      if (!requireNamespace("readxl", quietly = TRUE)) {
        stop("Package 'readxl' is required for loading contract types")
      }

      tipo_contratto <- readxl::read_xls(
        contract_path,
        sheet = "ST-TIPO CONTRATTI ",
        skip = 12
      ) |> data.table::setDT()

      data.table::setnames(tipo_contratto, "COD_TIPOCONTRATTI", "COD_TIPOLOGIA_CONTRATTUALE")
      tipo_contratto[, DTT_TMST := NULL]

      # Harmonize contract codes (consolidate similar types)
      tipo_contratto[, TIPOLOGIA_CONTRATTUALE := data.table::fcase(
        COD_TIPOLOGIA_CONTRATTUALE %in% c("A.03.00", "A.03.02", "A.03.11"), "A.03.09",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("A.03.01", "A.03.03", "A.03.12", "A.03.14"), "A.03.08",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("A.03.12", "A.03.13", "A.03.15"), "A.03.10",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("C.02.00"), "C.01.00",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("A.03.07"), "A.03.04",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("A.04.00", "A.04.01"), "A.04.02",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("A.05.00", "A.05.01"), "A.05.02",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("A.07.00", "A.07.01", "A.07.02"), "B.03.00",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("B.01.00", "B.02.00"), "B.03.00",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("G.01.00", "G.02.00"), "G.03.00",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("M.01.00", "M.01.01"), "M.02.00",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("F.01.00"), "A.01.00",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("F.02.00"), "A.02.00",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("H.01.00"), "H.03.00",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("A.08.00", "A.08.01"), "A.08.02",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("I.01.00"), "A.01.00",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("I.02.00"), "A.02.00",
        COD_TIPOLOGIA_CONTRATTUALE %in% c("L.01.00", "L.01.01"), "L.02.00",
        default = COD_TIPOLOGIA_CONTRATTUALE
      )]

      # Keep only harmonized codes (where code equals harmonized code)
      cla$etichette_contratti <- tipo_contratto[
        COD_TIPOLOGIA_CONTRATTUALE == TIPOLOGIA_CONTRATTUALE,
        .(COD_TIPOLOGIA_CONTRATTUALE = TIPOLOGIA_CONTRATTUALE, DES_TIPOCONTRATTI)
      ]

      cat("  Loaded", nrow(cla$etichette_contratti), "contract type labels\n")

    } else {
      warning("Contract types file not found at: ", contract_path)
      cla$etichette_contratti <- data.table::data.table(
        COD_TIPOLOGIA_CONTRATTUALE = character(0),
        DES_TIPOCONTRATTI = character(0)
      )
    }

    # 3. Load ATECO 3-digit sector labels
    ateco_path <- "../../progetti/all_base_scripts/data/Struttura-ATECO-2007-aggiornamento-2022.xlsx"

    if (file.exists(ateco_path)) {
      if (!requireNamespace("readxl", quietly = TRUE)) {
        stop("Package 'readxl' is required for loading ATECO codes")
      }

      ateco <- readxl::read_xlsx(ateco_path) |> data.table::setDT()
      names(ateco) <- c("ateco", "nome")
      ateco <- ateco[nchar(ateco) == 4]  # Keep only XX.X format (3-digit level)

      cla$settori <- ateco

      cat("  Loaded", nrow(cla$settori), "ATECO sector labels\n")

    } else {
      warning("ATECO codes file not found at: ", ateco_path)
      cla$settori <- data.table::data.table(
        ateco = character(0),
        nome = character(0)
      )
    }
  }

  return(cla)
}
