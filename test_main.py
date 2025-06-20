# test_main.py (nella stessa directory del tuo main.py)
import pytest
import pandas as pd
from unittest.mock import MagicMock

# Importa le funzioni che vuoi testare dal tuo main.py
# Supponiamo che tu abbia una funzione 'extract_info_from_doc_ai_output'
# e una funzione 'lookup_additional_data' nel tuo main.py
from main import process_document # Per un test più integrato della funzione cloud_event

# Mock dei client GCP per evitare chiamate reali
@pytest.fixture
def mock_gcp_clients():
    with (
        # Mock per Google Cloud Storage
        pytest.mock.patch('google.cloud.storage.Client') as MockStorageClient,
        pytest.mock.patch('google.cloud.storage.blob.Blob') as MockBlob,
        # Mock per Document AI
        pytest.mock.patch('google.cloud.documentai_v1.DocumentProcessorServiceClient') as MockDocumentAIClient
    ):
        mock_storage_client_instance = MockStorageClient.return_value
        mock_blob_instance = MockBlob.return_value
        mock_document_ai_client_instance = MockDocumentAIClient.return_value

        # Configura il mock del blob per il download
        mock_blob_instance.download_as_bytes.return_value = b"Contenuto di un documento di prova"
        mock_blob_instance.content_type = "application/pdf"

        # Configura il mock della risposta di Document AI
        mock_doc_ai_result = MagicMock()
        mock_doc_ai_result.document.entities = [
            MagicMock(type_="persona_nome", mention_text="Mario"),
            MagicMock(type_="persona_cognome", mention_text="Rossi"),
            MagicMock(type_="data_documento", mention_text="2025-06-20")
        ]
        mock_document_ai_client_instance.process_document.return_value = mock_doc_ai_result

        yield {
            "storage_client": mock_storage_client_instance,
            "documentai_client": mock_document_ai_client_instance,
            "blob": mock_blob_instance
        }

# Mock dei file di supporto
@pytest.fixture(autouse=True)
def mock_pandas_read_csv(monkeypatch):
    def mock_read_csv(filepath):
        if "Elenco Personale" in filepath:
            return pd.DataFrame({
                'Nome': ['Mario'],
                'Cognome': ['Rossi'],
                'ID Dipendente': ['DIP001'],
                'Email': ['mario.rossi@example.com']
            })
        elif "Docs Train" in filepath:
            return pd.DataFrame({
                'Categoria Doc': ['Contratto'],
                'Cluster ID': ['CLUSTER_A']
            })
        raise FileNotFoundError(f"Mocking error: {filepath} not found")

    monkeypatch.setattr(pd, 'read_csv', mock_read_csv)


def test_process_document_success(mock_gcp_clients, capsys):
    # Simula un evento Cloud Storage
    cloud_event_data = {
        "bucket": "test-input-bucket",
        "name": "test_document.pdf"
    }
    mock_cloud_event = MagicMock()
    mock_cloud_event.data = cloud_event_data

    # Imposta variabili d'ambiente per il test
    os.environ['GCP_PROJECT'] = 'test-project'
    os.environ['DOCUMENT_AI_LOCATION'] = 'test-location'
    os.environ['DOCUMENT_AI_PROCESSOR_ID'] = 'test-processor-id'
    os.environ['OUTPUT_BUCKET'] = 'test-output-bucket'

    # Esegui la funzione
    process_document(mock_cloud_event)

    # Verifica le chiamate ai client GCP mockati
    mock_gcp_clients["storage_client"].bucket.assert_called_with("test-input-bucket")
    mock_gcp_clients["blob"].download_as_bytes.assert_called_once()
    mock_gcp_clients["documentai_client"].processor_path.assert_called_with('test-project', 'test-location', 'test-processor-id')
    mock_gcp_clients["documentai_client"].process_document.assert_called_once()
    mock_gcp_clients["storage_client"].bucket.return_value.blob.return_value.upload_from_filename.assert_called_once()

    # Verifica l'output del log (opzionale, ma utile per debug)
    captured = capsys.readouterr()
    assert "Inizio elaborazione per il file: gs://test-input-bucket/test_document.pdf" in captured.out
    assert "File ZIP creato:" in captured.out
    assert "File di output caricato su:" in captured.out

    # Assicurati che i file temporanei siano stati puliti (dovresti aggiungere un test più specifico se vuoi)
    # Questa parte richiede di mockare os.remove e shutil.rmtree per verificarne la chiamata