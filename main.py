import os
import functions_framework # Necessario per le Cloud Functions
from google.cloud import storage, documentai_v1 as documentai, pubsub_v1
import json
import zipfile
import pandas as pd # Per gestire i file CSV Elenco Personale e Docs Train

# --- Configurazioni e Inizializzazione Client GCP ---
# Questi dovrebbero essere presi dalle variabili d'ambiente
PROJECT_ID = os.environ.get('GCP_PROJECT')
DOCUMENT_AI_LOCATION = os.environ.get('DOCUMENT_AI_LOCATION') # Es. 'europe-west1'
DOCUMENT_AI_PROCESSOR_ID = os.environ.get('DOCUMENT_AI_PROCESSOR_ID')
OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET') # Es. 'credemhack-output-iam'
# Potresti voler passare anche le credenziali o i dettagli di connessione a Cloud SQL
# o lasciare che le librerie gestiscano l'autenticazione tramite le variabili d'ambiente di Cloud Functions

storage_client = storage.Client()
documentai_client = documentai.DocumentProcessorServiceClient()
# Se invii notifiche Pub/Sub dal codice stesso (non solo tramite gcloud CLI per la sottomissione)
# publisher_client = pubsub_v1.PublisherClient()


# --- Caricamento Dati di Supporto ---
# Carica i file Elenco Personale e Docs Train all'avvio del container (una sola volta)
# Assicurati che questi file siano stati COPIATI nel Dockerfile nella directory /app/
try:
    elenco_personale_df = pd.read_csv('Elenco Personale.xlsx - Foglio 1.csv')
    cluster_docs_df = pd.read_csv('Docs Train.xlsx - Foglio1.csv')
    print("File di supporto 'Elenco Personale' e 'Docs Train' caricati con successo.")
except FileNotFoundError as e:
    print(f"Errore: file di supporto non trovato. Assicurati che siano nella stessa directory dell'applicazione nel container. Errore: {e}")
    # Gestisci l'errore o esci se i file sono critici

# --- Funzione Principale della Cloud Function ---
@functions_framework.cloud_event
def process_document(cloud_event):
    """
    Funzione Cloud attivata dal caricamento di un nuovo documento in GCS.
    Elabora il documento, estrae informazioni e genera l'output finale.
    """
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    file_uri = f"gs://{bucket_name}/{file_name}"

    print(f"Inizio elaborazione per il file: {file_uri}")

    try:
        # 1. Scarica il documento da GCS
        blob = storage_client.bucket(bucket_name).blob(file_name)
        document_content = blob.download_as_bytes()
        mime_type = blob.content_type
        print(f"Documento {file_name} scaricato. MIME Type: {mime_type}")

        # 2. Invocare Document AI per estrazione e classificazione
        # Questa parte è la più complessa e dipenderà dal tuo modello Document AI
        processor_name = documentai_client.processor_path(
            PROJECT_ID, DOCUMENT_AI_LOCATION, DOCUMENT_AI_PROCESSOR_ID
        )
        raw_document = documentai.RawDocument(content=document_content, mime_type=mime_type)
        request = documentai.ProcessRequest(name=processor_name, raw_document=raw_document)
        result = documentai_client.process_document(request=request)
        document_ai_output = result.document

        # --- Esempio di Logica di Estrazione da Document AI (da adattare al tuo processore) ---
        extracted_name = "Nome Non Trovato"
        extracted_surname = "Cognome Non Trovato"
        extracted_date = "Data Non Trovata"
        document_category = "Categoria Sconosciuta"
        
        # Esempio per processori di Form Parser o Custom Extractor:
        for entity in document_ai_output.entities:
            if entity.type_ == "persona_nome": # Adatta il nome del campo al tuo processore Document AI
                extracted_name = entity.mention_text
            elif entity.type_ == "persona_cognome":
                extracted_surname = entity.mention_text
            elif entity.type_ == "data_documento":
                extracted_date = entity.mention_text
            # Se hai un classificatore:
            # if entity.type_ == "document_type":
            #     document_category = entity.mention_text

        # --- Esempio per processori di Classificazione (se usi un classificatore separato) ---
        # if document_ai_output.document_type:
        #    document_category = document_ai_output.document_type
        
        print(f"Estratto: Nome={extracted_name}, Cognome={extracted_surname}, Data={extracted_date}, Categoria={document_category}")

        # 3. Recupera dati aggiuntivi da Elenco Personale e Cluster Docs
        # Usa extracted_name e extracted_surname per cercare in elenco_personale_df
        # e document_category per cluster_docs_df
        # Questo è un esempio semplificato di join o lookup
        unique_id = "GENERATED_ID_" + str(hash(file_name)) # Genera un ID univoco
        
        # Logica per recuperare altri campi da Elenco Personale e Cluster Docs
        # Es: Cerca un match per nome e cognome in elenco_personale_df
        matched_person = elenco_personale_df[
            (elenco_personale_df['Nome'].str.contains(extracted_name, case=False)) &
            (elenco_personale_df['Cognome'].str.contains(extracted_surname, case=False))
        ]
        
        # Questo è un placeholder, dovrai implementare la logica esatta di matching e recupero dati
        additional_info_person = matched_person.iloc[0].to_dict() if not matched_person.empty else {}
        print(f"Informazioni aggiuntive persona: {additional_info_person}")

        # 4. Compone il file .dat e la cartella BLOBFILES
        # Il file .dat deve contenere le informazioni estratte e quelle fisse.
        dat_content = f"Nome: {extracted_name}\n"
        dat_content += f"Cognome: {extracted_surname}\n"
        dat_content += f"Data di redazione: {extracted_date}\n"
        dat_content += f"Codice Identificativo Univoco: {unique_id}\n"
        dat_content += "Informazioni Fisse: CredemHack2025\n" # Come da specifiche del challenge
        
        # Aggiungi le informazioni recuperate dai file Excel/CSV
        for k, v in additional_info_person.items():
            dat_content += f"{k}: {v}\n"

        # Crea directory temporanee per lo zip
        temp_dir = "/tmp"
        blobfiles_temp_dir = os.path.join(temp_dir, "BLOBFILES")
        os.makedirs(blobfiles_temp_dir, exist_ok=True)

        # Scrive il file .dat
        dat_file_name = f"DocumentsOfRecord_{unique_id}.dat"
        dat_file_path = os.path.join(temp_dir, dat_file_name)
        with open(dat_file_path, "w") as f:
            f.write(dat_content)
        print(f"File .dat creato: {dat_file_path}")

        # Copia il documento originale nella cartella BLOBFILES
        original_doc_destination_path = os.path.join(blobfiles_temp_dir, os.path.basename(file_name))
        with open(original_doc_destination_path, "wb") as f:
            f.write(document_content)
        print(f"Documento originale copiato in BLOBFILES: {original_doc_destination_path}")

        # 5. Crea il file .zip
        zip_file_name = f"{os.path.splitext(file_name)[0]}_{unique_id}_output.zip"
        zip_file_path = os.path.join(temp_dir, zip_file_name)

        with zipfile.ZipFile(zip_file_path, 'w') as zipf:
            zipf.write(dat_file_path, os.path.basename(dat_file_path)) # Aggiungi il .dat alla radice dello zip
            # Aggiungi tutti i file dalla cartella BLOBFILES dentro la cartella BLOBFILES dello zip
            for root, _, files in os.walk(blobfiles_temp_dir):
                for file in files:
                    full_path = os.path.join(root, file)
                    arcname = os.path.join("BLOBFILES", os.path.basename(full_path))
                    zipf.write(full_path, arcname)
        print(f"File ZIP creato: {zip_file_path}")

        # 6. Carica il file .zip nel bucket di output
        output_blob = storage_client.bucket(OUTPUT_BUCKET).blob(zip_file_name)
        output_blob.upload_from_filename(zip_file_path)
        print(f"File di output caricato su: gs://{OUTPUT_BUCKET}/{zip_file_name}")

        # 7. Registra i metadati in BigQuery (DA IMPLEMENTARE)
        # Qui dovresti inserire una riga nella tua tabella BigQuery con:
        # - il nome del file originale
        # - l'URL del file ZIP di output
        # - Nome, Cognome, Data estratti
        # - Codice Identificativo Univoco
        # - Eventuali altri dati recuperati o metadati di elaborazione
        print("Logica per la registrazione in BigQuery da implementare qui.")

    except Exception as e:
        print(f"Errore critico durante l'elaborazione del file {file_uri}: {e}")
        # Aggiungi qui una logica per la gestione degli errori, es. notifica o spostamento del file in un bucket di errori.
    finally:
        # Pulisci i file temporanei
        if os.path.exists(dat_file_path):
            os.remove(dat_file_path)
        if os.path.exists(zip_file_path):
            os.remove(zip_file_path)
        if os.path.exists(blobfiles_temp_dir):
            import shutil
            shutil.rmtree(blobfiles_temp_dir) # Rimuove la directory e il suo contenuto
        print("Pulizia dei file temporanei completata.")