# Chord DHT

Implementazione in Python del protocollo Chord per Distributed Hash Table, con tolleranza ai guasti, replicazione dei dati e comunicazione crittografata.

## Panoramica

Chord è un protocollo di lookup distribuito che fornisce un modo scalabile ed efficiente per localizzare dati in una rete peer-to-peer. Questa implementazione segue il paper originale di Stoica et al., con funzionalità aggiuntive per tolleranza ai guasti e sicurezza.

### Funzionalità Principali

- **Distributed Hash Table**: Complessità di lookup O(log N) tramite finger table
- **Tolleranza ai Guasti**: Rilevamento automatico dei fallimenti e recovery con liste di successori
- **Replicazione Dati**: Fattore di replicazione configurabile per la durabilità dei dati
- **Comunicazione Crittografata**: Crittografia Fernet con firma HMAC dei messaggi
- **Gestione Churn**: Simulazione dinamica di join/leave dei nodi
- **Testing Completo**: Test di scalabilità, crash recovery e stabilità al churn

## Architettura

```
chord-dht/
├── core/
│   ├── ChordNode.py          # Implementazione principale del nodo
│   ├── FingerTable.py        # Tabella di routing O(log N)
│   ├── TopologyManager.py    # Topologia dell'anello e stabilizzazione
│   ├── DataStore.py          # Storage locale chiave-valore
│   ├── DataTransferManager.py# Migrazione chiavi tra nodi
│   └── NodeRef.py            # Astrazione riferimento nodo
├── network/
│   ├── SocketServer.py       # Server TCP asincrono
│   ├── RpcClient.py          # Chiamate a procedura remota
│   └── MessageProtocol.py    # Serializzazione messaggi
├── fault_tolerance/
│   └── FailureDetector.py    # Rilevamento guasti basato su ping
├── security/
│   └── Encryption.py         # Crittografia Fernet + HMAC
├── churn/
│   └── ChurnSimulator.py     # Simulazione dinamica join/leave
├── config/
│   ├── Settings.py           # Parametri di configurazione
│   └── LoggingConfig.py      # Configurazione logging
├── utils/
│   └── ChordMath.py          # Funzioni hash e controlli intervalli
├── tests/
│   ├── BenchmarkGraphs.py    # Suite benchmark con generazione grafici
│   ├── TestScalability.py    # Test scalabilità
│   ├── TestCrash.py          # Test crash recovery
│   ├── TestChurn.py          # Test stabilità al churn
│   ├── TestNetwork.py        # Test livello rete
│   └── TestEncryption.py     # Test sicurezza
└── main.py                   # Entry point del nodo
```

## Installazione

### Requisiti

- Python 3.8+

### Setup

```bash
pip install -r requirements.txt
```

## Configurazione

Modifica `config/Settings.py` per personalizzare:

```python
class NetworkSettings:
    IP_ADDRESS = '127.0.0.1'
    STARTING_PORT = 5000
    TIMEOUT = 2.0
    MAX_RETRIES = 3

class ChordSettings:
    M_BIT = 256                    # Dimensione spazio hash (2^256)
    STABILIZE_INTERVAL = 2         # Intervallo stabilizzazione (secondi)
    FIX_FINGERS_INTERVAL = 2       # Intervallo aggiornamento finger table
    CHECK_PREDECESSOR_INTERVAL = 2 # Intervallo controllo predecessore
    REPLICATION_FACTOR = 3         # Numero di repliche per chiave

class FailureDetectorSettings:
    PING_INTERVAL = 1              # Frequenza ping (secondi)
    FAILURE_THRESHOLD = 3          # Fallimenti prima di dichiarare nodo morto
    TIMEOUT = 1.0                  # Timeout ping

class SecuritySettings:
    SECRET_KEY = "your_secret_key"
    ENCRYPTION_ENABLED = True
    SIGNATURE_ENABLED = True
```

## Utilizzo

Per eseguire l'applicazione, posizionarsi nella cartella `tests/`:

```bash
cd tests
```

### Test Scalabilità

Misura le performance di GET/STORE al crescere della rete (10-50 nodi):

```bash
python TestScalability.py
```

### Test Crash Recovery

Testa il recupero dei dati dopo il fallimento dei nodi:

```bash
python TestCrash.py
```

### Test Churn

Testa la stabilità della rete durante eventi dinamici di join/leave:

```bash
python TestChurn.py
```

### Test Rete

Verifica il corretto funzionamento del livello di rete:

```bash
python TestNetwork.py
```

### Test Crittografia

Verifica il corretto funzionamento della crittografia e firma dei messaggi:

```bash
python TestEncryption.py
```

### Benchmark Completo con Grafici

Esegue tutti i test principali (scalabilità, crash, churn) e genera grafici delle performance:

```bash
python BenchmarkGraphs.py
```

**File di output:**
- `graph_scalability.png` - Latenza vs dimensione rete
- `graph_fault_tolerance.png` - Tasso di recupero vs nodi crashati
- `graph_churn.png` - Nodi attivi nel tempo
- `graphs_combined.png` - Tutte le metriche in un'unica figura

## Dettagli del Protocollo

### Anello Chord

I nodi sono disposti in un anello logico basato sul loro hash SHA-256. Ogni nodo mantiene:

- **Successore**: Nodo successivo nell'anello
- **Predecessore**: Nodo precedente nell'anello
- **Finger Table**: Scorciatoie di routing O(log N)
- **Lista Successori**: Successori di backup per tolleranza ai guasti

### Stabilizzazione

Task in background mantengono continuamente la consistenza dell'anello:

1. **Stabilize**: Verifica e aggiorna le relazioni con i successori
2. **Fix Fingers**: Aggiorna le entry della finger table
3. **Check Predecessor**: Rileva fallimenti del predecessore

### Rilevamento Guasti

Il failure detector usa monitoraggio basato su heartbeat:

1. Ping periodici a successore e predecessore
2. Soglia di fallimento configurabile
3. Recovery automatico usando lista successori o finger table

### Replicazione Dati

Le chiavi sono replicate su N nodi successori (configurabile):

1. Storage primario sul nodo responsabile
2. Storage replica sui successivi N-1 successori
3. Recovery automatico delle repliche in caso di fallimento nodo

## Sicurezza

Tutte le comunicazioni di rete sono protette con:

- **Crittografia Fernet**: Crittografia simmetrica usando AES-128-CBC
- **Firma HMAC**: Verifica integrità messaggi con SHA-256
- **Derivazione Chiave**: Chiave segreta hashata con SHA-256 per la chiave di crittografia