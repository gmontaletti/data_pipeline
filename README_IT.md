# Pipeline di Analisi delle Transizioni nel Mercato del Lavoro

Una pipeline completa di preprocessing dei dati per l'analisi delle transizioni nel mercato del lavoro, dei periodi di disoccupazione e dell'efficacia delle politiche attive del lavoro in Lombardia, Italia.

**Autore**: Giampaolo Montaletti
**Email**: giampaolo.montaletti@gmail.com
**GitHub**: [github.com/gmontaletti](https://github.com/gmontaletti)
**ORCID**: [0009-0002-5327-1122](https://orcid.org/0009-0002-5327-1122)

## Panoramica

Questa pipeline trasforma i dati amministrativi sull'occupazione in dataset analitici pronti per la visualizzazione in dashboard. Calcola le transizioni da impiego a impiego (transizioni chiuse), le transizioni da impiego a disoccupazione (transizioni aperte), le arricchisce con informazioni sulle dichiarazioni di disoccupazione e politiche attive del lavoro (DID - Dichiarazione di Immediata Disponibilità, POL - Politiche Attive) e produce aggregati completi per la visualizzazione interattiva.

### Architettura a Doppio Ramo

La pipeline elabora i dati attraverso **DUE rami paralleli** per consentire diverse prospettive analitiche:

- **Ramo Residenza**: Filtra per residenza del lavoratore (`COMUNE_LAVORATORE`) → Output in `output/dashboard/`
  - Include i lavoratori domiciliati in Lombardia, indipendentemente dal luogo di lavoro
  - Una persona che vive in Lombardia e lavora fuori regione è **inclusa**
  - Una persona che vive fuori dalla Lombardia e lavora in Lombardia è **esclusa**

- **Ramo Sede di Lavoro**: Filtra per ubicazione del posto di lavoro (`COMUNE_SEDE_LAVORO`) → Output in `output/dashboard_workplace/`
  - Include i posti di lavoro ubicati in Lombardia, indipendentemente dalla residenza del lavoratore
  - Una persona che vive fuori dalla Lombardia e lavora in Lombardia è **inclusa**
  - Una persona che vive in Lombardia e lavora fuori regione è **esclusa**

Entrambi i rami producono strutture di file identiche e subiscono la stessa elaborazione analitica dopo la fase di filtraggio.

### Caratteristiche Principali

- **Elaborazione a Doppio Ramo**: Analisi parallela per residenza e sede di lavoro
- **Analisi delle Transizioni**: Calcola transizioni chiuse (da lavoro a lavoro) e aperte (da lavoro a disoccupazione)
- **Arricchimento con Politiche**: Collega le transizioni al supporto delle politiche attive DID e POL
- **Metriche di Carriera**: Analisi di sopravvivenza, indici di stabilità occupazionale, tassi di turnover
- **Clustering delle Carriere**: Raggruppa gli individui per pattern di traiettoria professionale
- **Aggregazioni Geografiche**: Sintesi per area geografica e CPI (Centro Per l'Impiego)
- **Serie Temporali**: Aggregati mensili per l'analisi delle tendenze
- **Analisi con Lag di 45 Giorni**: Filtra le transizioni rapide per concentrarsi su cambi di carriera significativi
- **Visualizzazione a Rete**: Grafi di rete per transizioni professionali e settoriali

## Architettura della Pipeline

La pipeline utilizza il framework `targets` per orchestrare un **workflow a DOPPIO RAMO** con 13 fasi (Fase 0-12):

### Preprocessing Condiviso (Prima del Fork)
- **Fase 0**: Caricamento Dati Grezzi - Carica `rap.fst` (contratti), `did.fst` (dichiarazioni di disoccupazione), `pol.fst` (politiche attive del lavoro)
- **Fase 1**: Preparazione Dati - Armonizza i codici contrattuali, standardizza i livelli di istruzione, seleziona le colonne rilevanti
- **Fase 2**: Fork - Applica filtri di localizzazione per creare due rami paralleli

### Elaborazione dei Rami Paralleli (Sia Residenza che Sede di Lavoro)
Ogni ramo viene quindi elaborato attraverso:
- **Fase 3**: Consolidamento e Arricchimento - Consolidamento vecshift, aggiunge periodi di disoccupazione, abbina eventi DID/POL, consolidamento LongworkR, aggiunge arricchimento CPI/ATECO
- **Fase 4**: Calcolo Transizioni - Estrae dati demografici, calcola transizioni chiuse e aperte, arricchisce con DID/POL
- **Fase 5**: Creazione Matrici di Transizione - Aggrega per tipo di contratto, professione, settore (standard + lag 45 giorni)
- **Fase 6**: Metriche di Carriera e Sopravvivenza - Calcola curve di sopravvivenza e metriche di traiettoria
- **Fase 7**: Clustering delle Carriere - Raggruppa le traiettorie, crea dataset a livello di persona
- **Fase 8**: Aggregazioni Geografiche - Sintesi per area e CPI
- **Fase 9**: Serie Temporali - Aggregati mensili
- **Fase 10**: Sintesi delle Politiche - Analisi dell'efficacia DID/POL
- **Fase 11**: Precompilazioni Copertura Politiche - Ottimizzazioni delle prestazioni della dashboard
- **Fase 12**: Output dei File - Scrive in directory di output separate (`output/dashboard/` e `output/dashboard_workplace/`)

## Prerequisiti

### Pacchetti R Richiesti

```r
install.packages(c("targets", "tarchetypes", "data.table", "fst", "devtools", "ggplot2"))
```

### Dipendenze Esterne

- **Pacchetto vecshift** (versione di sviluppo): Deve essere installato da sorgente locale in `~/Documents/funzioni/vecshift/`
- **Pacchetto longworkR** (versione di sviluppo): Deve essere installato da sorgente locale in `~/Documents/funzioni/longworkR/`
- **Directory dati condivisi**: Dati sull'occupazione grezzi in `~/Documents/funzioni/shared_data/` (o impostare `$SHARED_DATA_DIR`)

### Requisiti dei Dati

La pipeline richiede i seguenti file in `$SHARED_DATA_DIR/raw/`:
- `rap.fst` - Contratti di lavoro grezzi (tutti i campi, prima di qualsiasi filtraggio)
- `did.fst` - Dichiarazioni di disoccupazione (dati DID/NASPI)
- `pol.fst` - Partecipazione alle politiche attive del lavoro

File aggiuntivi richiesti:
- `comune_cpi_lookup.rds` - Tabella di lookup geografica in `$SHARED_DATA_DIR/maps/comune_cpi_lookup.rds`
- Tabelle di lookup dei classificatori (caricate tramite la funzione `load_classifiers()`)

## Installazione

```bash
# Clonare il repository
git clone https://github.com/gmontaletti/data_pipeline.git
cd data_pipeline

# Assicurarsi che i pacchetti vecshift e longworkR siano disponibili
# (Installare da sorgente locale se necessario)
# R CMD INSTALL ~/Documents/funzioni/vecshift/
# R CMD INSTALL ~/Documents/funzioni/longworkR/

# Impostare la directory dati condivisi (opzionale, default è ~/Documents/funzioni/shared_data)
export SHARED_DATA_DIR="/percorso/ai/dati/condivisi"
```

## Utilizzo

### Esecuzione della Pipeline Completa

```bash
# Eseguire la pipeline completa
./update_data.sh
```

Questo esegue `tar_make()` e visualizza il rapporto di validazione.

### Monitoraggio del Progresso

```bash
# Verificare il progresso della pipeline in tempo reale
./check_progress.sh
```

### Utilizzo dalla Console R

```r
library(targets)

# Eseguire l'intera pipeline
tar_make()

# Eseguire target specifici
tar_make(transitions)

# Visualizzare il grafo delle dipendenze della pipeline
tar_visnetwork()

# Verificare quali target necessitano di ricostruzione
tar_outdated()

# Leggere un target calcolato
tar_read(person_data)

# Invalidare un target specifico per forzare la ricostruzione
tar_invalidate(transitions)
```

### Workflow di Sviluppo

```r
# Ricaricare le funzioni dopo aver modificato gli script in R/
devtools::load_all("R/")

# Ricostruire target specifici dopo modifiche alle funzioni
tar_invalidate(c("transitions", "transition_matrices"))
tar_make()

# Pulire tutti i target (usare con cautela!)
tar_destroy()
```

## Struttura degli Output

La pipeline produce output in **DUE directory parallele**:

- **`output/dashboard/`**: Analisi basata sulla residenza (filtrata per `COMUNE_LAVORATORE`)
- **`output/dashboard_workplace/`**: Analisi basata sulla sede di lavoro (filtrata per `COMUNE_SEDE_LAVORO`)

Entrambe le directory contengono strutture di file identiche:

### Dataset di Grandi Dimensioni (formato FST)
- `transitions.fst` - Tutte le transizioni con attributi completi (oltre 108M di record)
- `person_data.fst` - Sintesi a livello di persona con dati demografici e metriche di carriera
- `monthly_timeseries.fst` - Aggregati mensili per area
- `monthly_timeseries_cpi.fst` - Aggregati mensili per CPI

### Tabelle di Lookup e Sintesi (formato RDS)
- `classifiers.rds` - Tabelle di lookup per contratto/professione/settore
- `survival_curves.rds` - Risultati dell'analisi di sopravvivenza
- `transition_matrices.rds` / `transition_matrices_8day.rds` - Matrici di transizione standard e con lag di 45 giorni
- `profession_transitions.rds` / `sector_transitions.rds` - Liste di archi di transizione etichettati
- `profession_matrix.rds` / `sector_matrix.rds` - Formato matrice R per operazioni
- `profession_summary_stats.rds` / `sector_summary_stats.rds` - Principali transizioni, indici di mobilità
- `geo_summary.rds` / `geo_summary_cpi.rds` - Aggregati geografici
- `policy_*.rds` - Aggregati precompilati della copertura delle politiche (serie temporali, dati demografici, geografia, distribuzioni di durata)

### Visualizzazioni (formato PNG)
- `plots/profession_network.png` - Rete di transizioni professionali
- `plots/sector_network.png` - Rete di transizioni settoriali economiche
- `plots/profession_network_8day.png` - Rete professionale con lag di 45 giorni
- `plots/sector_network_8day.png` - Rete settoriale con lag di 45 giorni

## Struttura del Progetto

```
funzioni/
├── data_pipeline/                  # Progetto principale (repository git)
│   ├── R/                          # Moduli di funzioni
│   │   ├── data_preparation.R     # Caricamento dati grezzi, armonizzazione, filtraggio per localizzazione
│   │   ├── data_loading.R         # Consolidamento dati (vecshift + longworkR)
│   │   ├── transitions.R          # Calcolo principale delle transizioni
│   │   ├── aggregations.R         # Aggregati a livello di persona e geografici
│   │   ├── policy_aggregates.R    # Copertura delle politiche precompilata
│   │   ├── career_analysis.R      # Metriche di sopravvivenza e carriera
│   │   ├── classifiers.R          # Lookup di classificazione
│   │   ├── transition_enrichment.R # Etichette e visualizzazioni
│   │   └── utils_geographic.R     # Utilità geografiche
│   ├── _targets.R                  # Definizione della pipeline
│   ├── _targets/                   # Oggetti intermedi (gitignored)
│   ├── output/                     # File di output (gitignored)
│   │   ├── dashboard/             # Output analisi basata su residenza
│   │   └── dashboard_workplace/   # Output analisi basata su sede di lavoro
│   ├── check_progress.sh          # Script di monitoraggio progresso
│   ├── update_data.sh             # Script di esecuzione pipeline
│   ├── CLAUDE.md                   # Guida per Claude Code
│   ├── README.md                   # File in inglese
│   ├── README_IT.md                # Questo file
│   └── .gitignore                  # Esclusioni git
├── reference/                      # Materiali di riferimento (non in git)
│   └── data_pipeline/             # Riferimenti specifici del progetto
│       ├── docs/                  # Documentazione tecnica
│       ├── papers/                # Articoli di ricerca
│       ├── notes/                 # Note di sviluppo
│       └── README.md              # Guida directory di riferimento
├── shared_data/                    # Directory dati condivisi
│   ├── raw/                       # Dati di input grezzi
│   │   ├── rap.fst               # Contratti di lavoro
│   │   ├── did.fst               # Dichiarazioni di disoccupazione
│   │   └── pol.fst               # Politiche attive del lavoro
│   └── maps/                      # Tabelle di lookup geografiche
│       └── comune_cpi_lookup.rds  # Mappatura CPI
├── vecshift/                       # Dipendenza pacchetto di sviluppo
└── longworkR/                      # Dipendenza pacchetto di sviluppo
```

## Concetti Chiave

### Filtraggio per Residenza vs Sede di Lavoro

L'architettura a doppio ramo consente diverse prospettive analitiche:

**Ramo Residenza** (filtrato per `COMUNE_LAVORATORE`):
- Traccia gli individui **domiciliati in Lombardia**
- Include il loro impiego indipendentemente da dove lavorano
- Caso d'uso: Analisi della forza lavoro regionale, pattern di mobilità dei lavoratori
- Esempio: Un residente a Milano che lavora a Torino è **incluso**

**Ramo Sede di Lavoro** (filtrato per `COMUNE_SEDE_LAVORO`):
- Traccia i posti di lavoro **ubicati in Lombardia**
- Include tutti i lavoratori in queste posizioni indipendentemente dalla residenza
- Caso d'uso: Analisi dell'economia regionale, domanda di lavoro in Lombardia
- Esempio: Un residente in Piemonte che lavora a Milano è **incluso**

Copertura geografica per entrambi i rami: Tutte le 12 province lombarde (Milano, Brescia, Bergamo, Monza e Brianza, Varese, Como, Mantova, Pavia, Cremona, Lecco, Sondrio, Lodi)

### Tipi di Transizione

- **Transizione Chiusa**: Impiego → Disoccupazione → Impiego (ha un lavoro di destinazione)
- **Transizione Aperta**: Impiego → Disoccupazione in Corso (nessun lavoro di destinazione alla fine del periodo di osservazione)

### Analisi con Lag di 45 Giorni

La pipeline calcola due versioni delle matrici di transizione:
- **Standard**: Tutte le transizioni incluse
- **Lag di 45 Giorni**: Filtra le durate di disoccupazione >45 giorni, escludendo le riassunzioni rapide per concentrarsi su cambiamenti di carriera significativi

### Supporto delle Politiche (DID/POL)

- **DID (Dichiarazione di Immediata Disponibilità)**: Sistema di dichiarazione di disoccupazione che registra gli individui come disponibili al lavoro
- **POL (Politiche Attive)**: Politiche Attive del Mercato del Lavoro - interventi che forniscono formazione, supporto occupazionale e altre misure attive
- Le flag delle politiche vengono abbinate ai periodi di disoccupazione tramite join non-equi con i record dei periodi di disoccupazione

## Workflow Git

### Configurazione Iniziale

```bash
# Inizializzare git (già fatto se hai clonato)
git init

# Aggiungere tutti i file (rispetta .gitignore)
git add .

# Creare il commit iniziale
git commit -m "Commit iniziale: Pipeline transizioni mercato del lavoro"

# Aggiungere repository remoto
git remote add origin https://github.com/gmontaletti/data_pipeline.git

# Fare push su GitHub
git push -u origin main
```

### Sviluppo Regolare

```bash
# Verificare lo stato
git status

# Preparare le modifiche (stage)
git add R/transitions.R _targets.R

# Commit con messaggio descrittivo
git commit -m "Aggiunge calcolo matrice di transizione con lag di 45 giorni"

# Push verso il remoto
git push

# Pull delle ultime modifiche
git pull
```

### Creazione di Branch di Funzionalità

```bash
# Creare e passare a un nuovo branch
git checkout -b feature/miglioramenti-copertura-politiche

# Apportare modifiche e commit
git add .
git commit -m "Migliora il calcolo della copertura delle politiche"

# Fare push del branch al remoto
git push -u origin feature/miglioramenti-copertura-politiche

# Creare pull request su GitHub, quindi fare merge ed eliminare il branch
git checkout main
git pull
git branch -d feature/miglioramenti-copertura-politiche
```

## Risoluzione dei Problemi

### Pipeline Bloccata

Verificare il progresso e terminare i processi bloccati:
```bash
./check_progress.sh
# Se bloccato, identificare e terminare il processo R
ps aux | grep R
kill <pid>
```

### Dati CPI Mancanti

Assicurarsi che la tabella di lookup CPI esista:
```bash
ls ~/Documents/funzioni/shared_data/maps/comune_cpi_lookup.rds
```

### Dipendenze dei Pacchetti Non Trovate

Verificare che i percorsi in `_targets.R` corrispondano alla propria installazione:
```r
devtools::load_all("~/Documents/funzioni/vecshift/")
devtools::load_all("~/Documents/funzioni/longworkR/")
```

### Codici CPI Obsoleti

Se compaiono nuovi codici obsoleti, aggiungere la logica di ricodifica nella Fase 1.3 di `_targets.R`:
```r
data_with_geo[cpi_code == "CODICE_VECCHIO", cpi_code := "CODICE_NUOVO"]
```

## Note sulle Prestazioni

- **Formato Dati**: FST per dataset di grandi dimensioni (>10M record), RDS per piccole lookup
- **Compressione**: Livello di compressione FST 85 bilancia velocità e dimensione
- **Memoria**: La pipeline richiede circa 8-16GB di RAM per l'esecuzione completa
- **Tempo di Esecuzione**: L'esecuzione completa della pipeline richiede 30-60 minuti a seconda della dimensione dei dati

## Versione

**Versione Corrente**: 0.2.2 (2025-11-06)

- v0.2.2: Correzione del bug filter_by_location() e ottimizzazione delle prestazioni (6-30x più veloce, Fase 2 ora ~20s)
- v0.2.1: Correzioni di bug per la gestione dei tipi Date/IDate e per l'abbinamento DID/POL
- v0.2.0: Implementazione architettura a doppio ramo
  - Aggiunta elaborazione parallela per residenza (COMUNE_LAVORATORE) e sede di lavoro (COMUNE_SEDE_LAVORO)
  - Directory di output doppie: output/dashboard/ e output/dashboard_workplace/
  - Aggiunto modulo data_preparation.R per il filtraggio per localizzazione
  - Aggiunta dipendenza pacchetto vecshift
  - Ristrutturazione della pipeline in 13 fasi (0-12) con fork alla Fase 2
- v0.1.0: Rilascio iniziale con elaborazione a ramo singolo

## Citazione

Se utilizzi questa pipeline nella tua ricerca, per favore cita:

```
Montaletti, G. (2025). Labor Market Transition Analysis Pipeline (v0.2.2).
GitHub: https://github.com/gmontaletti/data_pipeline
ORCID: 0009-0002-5327-1122
```

## Licenza

[Specificare qui la propria licenza, es. MIT, GPL-3, ecc.]

## Contributi

I contributi sono benvenuti! Per favore:
1. Fai fork del repository
2. Crea un branch di funzionalità (`git checkout -b feature/funzionalita-straordinaria`)
3. Esegui il commit delle tue modifiche (`git commit -m 'Aggiunge funzionalità straordinaria'`)
4. Fai push al branch (`git push origin feature/funzionalita-straordinaria`)
5. Apri una Pull Request

## Contatti

Per domande o richieste di collaborazione:

**Giampaolo Montaletti**
Email: giampaolo.montaletti@gmail.com
GitHub: [@gmontaletti](https://github.com/gmontaletti)
