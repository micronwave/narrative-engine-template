\# Narrative Intelligence Engine — Architecture Contract



\*\*This file is the single source of truth. All implementations MUST conform to these interfaces exactly.\*\*



\*\*DO NOT:\*\*

\- Add methods not defined here

\- Change method signatures

\- Alter return types

\- Skip abstract method implementations



\*\*DO:\*\*

\- Read this file at the start of every build phase

\- Verify conformance after each phase

\- Flag any ambiguity before implementing



\---



\## File Structure



```

narrative\_engine/

├── ARCHITECTURE.md       # This file - read first

├── settings.py           # Phase 1 - Configuration

├── repository.py         # Phase 1 - Database abstraction

├── vector\_store.py       # Phase 1 - FAISS abstraction  

├── embedding\_model.py    # Phase 1 - Embedding abstraction

├── robots.py             # Phase 2 - robots.txt compliance

├── ingester.py           # Phase 2 - Data ingestion

├── deduplicator.py       # Phase 2 - LSH deduplication

├── clustering.py         # Phase 3 - HDBSCAN clustering

├── signals.py            # Phase 3 - Signal computation

├── centrality.py         # Phase 3 - Network analysis

├── adversarial.py        # Phase 3 - Coordination detection

├── llm\_client.py         # Phase 4 - LLM interaction

├── asset\_mapper.py       # Phase 4 - Asset mapping

├── output.py             # Phase 4 - Output generation

├── pipeline.py           # Phase 5 - Orchestration

├── build\_asset\_library.py # Phase 5 - Offline asset builder

├── requirements.txt      # Phase 6

├── README.md             # Phase 6

├── .env.example          # Phase 6

├── .env                  # User-created, not committed

└── data/

&#x20;   ├── narrative\_engine.db

&#x20;   ├── faiss\_index.pkl

&#x20;   ├── lsh\_index.pkl

&#x20;   ├── asset\_library.pkl

&#x20;   ├── tfidf\_vectorizer.pkl  # Hybrid mode only

&#x20;   ├── tfidf\_svd.pkl         # Hybrid mode only

&#x20;   └── outputs/

&#x20;       └── {date}/

&#x20;           └── narratives.json

```



\---



\## Cross-Module Dependencies



```

settings.py        → (none, base module)

repository.py      → settings

vector\_store.py    → settings

embedding\_model.py → settings

robots.py          → repository

ingester.py        → robots, repository

deduplicator.py    → settings

clustering.py      → repository, vector\_store, embedding\_model

signals.py         → (pure functions, no module dependencies)

centrality.py      → vector\_store

adversarial.py     → deduplicator, repository

llm\_client.py      → settings, repository

asset\_mapper.py    → embedding\_model

output.py          → repository

pipeline.py        → ALL MODULES

```



\---



\## Core Types



\### RawDocument (ingester.py)



```python

from dataclasses import dataclass



@dataclass

class RawDocument:

&#x20;   doc\_id: str              # UUID generated at ingestion

&#x20;   raw\_text: str

&#x20;   source\_url: str

&#x20;   source\_domain: str

&#x20;   published\_at: str        # ISO8601

&#x20;   ingested\_at: str         # ISO8601

&#x20;   author: str | None = None

&#x20;   raw\_text\_hash: str = ""  # SHA256 of raw\_text

```



\### AdversarialEvent (adversarial.py)



```python

from dataclasses import dataclass



@dataclass

class AdversarialEvent:

&#x20;   event\_id: str

&#x20;   affected\_narrative\_ids: list\[str]

&#x20;   source\_domains: list\[str]

&#x20;   similarity\_score: float

&#x20;   detected\_at: str  # ISO8601

```



\---



\## Abstract Interfaces



\### VectorStore (vector\_store.py)



```python

from abc import ABC, abstractmethod

import numpy as np



class VectorStore(ABC):

&#x20;   """

&#x20;   Abstract interface for vector storage operations.

&#x20;   MVP implementation: FaissVectorStore using IndexFlatIP.

&#x20;   

&#x20;   # TODO SCALE: swap FaissVectorStore for PgVectorStore or PineconeVectorStore when moving to AWS

&#x20;   """

&#x20;   

&#x20;   @abstractmethod

&#x20;   def add(self, vectors: np.ndarray, ids: list\[str]) -> None:

&#x20;       """Add vectors with associated IDs to the index."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def search(self, query\_vector: np.ndarray, k: int) -> tuple\[np.ndarray, list\[str]]:

&#x20;       """

&#x20;       Search for k nearest neighbors.

&#x20;       Returns: (distances, ids)

&#x20;       Must handle empty index: return (np.array(\[]), \[])

&#x20;       """

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def update(self, id: str, new\_vector: np.ndarray) -> None:

&#x20;       """Update the vector for an existing ID."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def delete(self, id: str) -> None:

&#x20;       """Remove a vector by ID."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def save(self) -> None:

&#x20;       """Persist the index to FAISS\_INDEX\_PATH."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def load(self) -> bool:

&#x20;       """

&#x20;       Load the index from FAISS\_INDEX\_PATH.

&#x20;       Returns False if file does not exist (do not raise).

&#x20;       """

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def count(self) -> int:

&#x20;       """Return the number of vectors in the index."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_vector(self, id: str) -> np.ndarray | None:

&#x20;       """Retrieve a vector by ID, or None if not found."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_all\_ids(self) -> list\[str]:

&#x20;       """Return all IDs in the index."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def initialize(self, dimension: int) -> None:

&#x20;       """Initialize a fresh empty index with the given dimension."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def is\_empty(self) -> bool:

&#x20;       """Return True if the index contains zero vectors."""

&#x20;       ...

```



\### Repository (repository.py)



```python

from abc import ABC, abstractmethod



class Repository(ABC):

&#x20;   """

&#x20;   Abstract interface for all database operations.

&#x20;   MVP implementation: SqliteRepository.

&#x20;   

&#x20;   # TODO SCALE: swap SqliteRepository for PostgresRepository (psycopg2) on AWS RDS — interface is identical

&#x20;   """

&#x20;   

&#x20;   @abstractmethod

&#x20;   def migrate(self) -> None:

&#x20;       """Create all tables if they do not exist."""

&#x20;       ...

&#x20;   

&#x20;   # --- Narrative Operations ---

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_narrative(self, narrative\_id: str) -> dict | None:

&#x20;       """Get a single narrative by ID."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_all\_active\_narratives(self) -> list\[dict]:

&#x20;       """Get all non-suppressed narratives."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def insert\_narrative(self, narrative: dict) -> None:

&#x20;       """Insert a new narrative record."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def update\_narrative(self, narrative\_id: str, updates: dict) -> None:

&#x20;       """Update fields on an existing narrative."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_narrative\_count(self) -> int:

&#x20;       """Get total count of all narratives (for consistency checks)."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_narratives\_by\_stage(self, stage: str) -> list\[dict]:

&#x20;       """Get all narratives with a specific stage."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_narratives\_needing\_decay(self, current\_date: str) -> list\[str]:

&#x20;       """Get narrative\_ids that received zero assignments on current\_date."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def record\_narrative\_assignment(self, narrative\_id: str, date: str) -> None:

&#x20;       """Record that a narrative received a document assignment on this date."""

&#x20;       ...

&#x20;   

&#x20;   # --- Candidate Buffer Operations ---

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_candidate\_buffer(self, status: str = 'pending') -> list\[dict]:

&#x20;       """Get all candidates with the given status."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def insert\_candidate(self, candidate: dict) -> None:

&#x20;       """Insert a document into the candidate buffer."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def update\_candidate\_status(self, doc\_id: str, status: str, narrative\_id\_assigned: str = None) -> None:

&#x20;       """Update the status of a candidate document."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_candidate\_buffer\_count(self, status: str = 'pending') -> int:

&#x20;       """Count candidates with the given status."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def clear\_candidate\_buffer(self, status: str = 'clustered') -> None:

&#x20;       """Delete all candidates with the given status."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def delete\_old\_candidate\_buffer(self, days: int) -> int:

&#x20;       """Delete clustered entries older than N days. Return count deleted."""

&#x20;       ...

&#x20;   

&#x20;   # --- Centroid History Operations ---

&#x20;   

&#x20;   @abstractmethod

&#x20;   def insert\_centroid\_history(self, narrative\_id: str, date: str, centroid\_blob: bytes) -> None:

&#x20;       """Store a centroid snapshot."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_centroid\_history(self, narrative\_id: str, days: int) -> list\[dict]:

&#x20;       """Get centroid history for a narrative, most recent first."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_latest\_centroid(self, narrative\_id: str) -> bytes | None:

&#x20;       """Get the most recent centroid blob for a narrative."""

&#x20;       ...

&#x20;   

&#x20;   # --- LLM Audit Operations ---

&#x20;   

&#x20;   @abstractmethod

&#x20;   def log\_llm\_call(self, call\_record: dict) -> None:

&#x20;       """Log an LLM call to the audit log."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_sonnet\_calls\_last\_24h(self, narrative\_id: str) -> list\[dict]:

&#x20;       """Get Sonnet calls for a narrative in the last 24 hours."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_sonnet\_daily\_spend(self, date: str) -> dict | None:

&#x20;       """Get the daily spend record for a date."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def update\_sonnet\_daily\_spend(self, date: str, tokens: int, calls: int) -> None:

&#x20;       """Update or insert the daily spend record."""

&#x20;       ...

&#x20;   

&#x20;   # --- Adversarial Operations ---

&#x20;   

&#x20;   @abstractmethod

&#x20;   def log\_adversarial\_event(self, event: dict) -> None:

&#x20;       """Log a coordination detection event."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_coordination\_flags\_rolling\_window(self, narrative\_id: str, days: int) -> int:

&#x20;       """Count coordination flags for a narrative in the last N days."""

&#x20;       ...

&#x20;   

&#x20;   # --- Robots Cache Operations ---

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_robots\_cache(self, domain: str) -> dict | None:

&#x20;       """Get cached robots.txt rules for a domain."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def set\_robots\_cache(self, domain: str, rules\_text: str, fetched\_at: str) -> None:

&#x20;       """Cache robots.txt rules for a domain."""

&#x20;       ...

&#x20;   

&#x20;   # --- Failed Job Operations ---

&#x20;   

&#x20;   @abstractmethod

&#x20;   def insert\_failed\_job(self, job: dict) -> None:

&#x20;       """Log a failed ingestion job."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_retryable\_failed\_jobs(self, current\_time: str) -> list\[dict]:

&#x20;       """Get jobs where next\_retry\_at <= current\_time and retry\_count < 3."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def update\_failed\_job\_retry(self, job\_id: str, retry\_count: int, next\_retry\_at: str) -> None:

&#x20;       """Update retry metadata for a failed job."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def delete\_failed\_job(self, job\_id: str) -> None:

&#x20;       """Remove a failed job record (after successful retry)."""

&#x20;       ...

&#x20;   

&#x20;   # --- Pipeline Run Log Operations ---

&#x20;   

&#x20;   @abstractmethod

&#x20;   def log\_pipeline\_run(self, run\_record: dict) -> None:

&#x20;       """Log a pipeline step execution."""

&#x20;       ...

&#x20;   

&#x20;   # --- Document Evidence Operations ---

&#x20;   

&#x20;   @abstractmethod

&#x20;   def insert\_document\_evidence(self, evidence: dict) -> None:

&#x20;       """Store supporting evidence for a narrative."""

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def get\_document\_evidence(self, narrative\_id: str) -> list\[dict]:

&#x20;       """Get all evidence documents for a narrative."""

&#x20;       ...

```



\### EmbeddingModel (embedding\_model.py)



```python

from abc import ABC, abstractmethod

import numpy as np



class EmbeddingModel(ABC):

&#x20;   """

&#x20;   Abstract interface for text embedding.

&#x20;   MVP implementation: MiniLMEmbedder.

&#x20;   """

&#x20;   

&#x20;   @abstractmethod

&#x20;   def embed(self, texts: list\[str]) -> np.ndarray:

&#x20;       """

&#x20;       Embed a list of texts.

&#x20;       Returns: np.ndarray of shape (len(texts), dimension)

&#x20;       All vectors must be L2-normalized.

&#x20;       """

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def embed\_single(self, text: str) -> np.ndarray:

&#x20;       """

&#x20;       Embed a single text.

&#x20;       Returns: np.ndarray of shape (dimension,)

&#x20;       Convenience method: calls embed(\[text])\[0]

&#x20;       """

&#x20;       ...

&#x20;   

&#x20;   @abstractmethod

&#x20;   def dimension(self) -> int:

&#x20;       """

&#x20;       Return the embedding dimension.

&#x20;       Dense mode: 384

&#x20;       Hybrid mode: 448

&#x20;       """

&#x20;       ...

```



\### Ingester (ingester.py)



```python

from abc import ABC, abstractmethod



class Ingester(ABC):

&#x20;   """

&#x20;   Abstract interface for data ingestion.

&#x20;   All implementations must check robots.txt via can\_fetch() before HTTP requests.

&#x20;   """

&#x20;   

&#x20;   @abstractmethod

&#x20;   def ingest(self) -> list\[RawDocument]:

&#x20;       """

&#x20;       Fetch and return raw documents.

&#x20;       Must populate all required RawDocument fields.

&#x20;       Must respect robots.txt.

&#x20;       """

&#x20;       ...

```



\### Deduplicator (deduplicator.py)



```python

class Deduplicator:

&#x20;   """

&#x20;   LSH-based deduplication using MinHashLSH.

&#x20;   

&#x20;   # TODO SCALE: replace pickle persistence with Redis backend for MinHashLSH when moving to multi-worker deployment

&#x20;   """

&#x20;   

&#x20;   def \_\_init\_\_(self, threshold: float, num\_perm: int, lsh\_path: str):

&#x20;       """Initialize with LSH parameters and persistence path."""

&#x20;       ...

&#x20;   

&#x20;   def load(self) -> bool:

&#x20;       """Load from disk. Returns False if file does not exist (fresh init)."""

&#x20;       ...

&#x20;   

&#x20;   def save(self) -> None:

&#x20;       """Persist to disk."""

&#x20;       ...

&#x20;   

&#x20;   def is\_duplicate(self, doc: RawDocument) -> bool:

&#x20;       """Check if document is a duplicate. Handles empty index."""

&#x20;       ...

&#x20;   

&#x20;   def add(self, doc: RawDocument) -> None:

&#x20;       """Add document signature to the index."""

&#x20;       ...

&#x20;   

&#x20;   def get\_signature(self, doc: RawDocument) -> 'MinHash':

&#x20;       """Compute MinHash signature for a document."""

&#x20;       ...

&#x20;   

&#x20;   def get\_batch\_signatures(self) -> dict\[str, 'MinHash']:

&#x20;       """Get signatures for current batch (for adversarial detection)."""

&#x20;       ...

&#x20;   

&#x20;   def clear\_batch(self) -> None:

&#x20;       """Clear the current batch signatures."""

&#x20;       ...

```



\### LlmClient (llm\_client.py)



```python

class LlmClient:

&#x20;   """

&#x20;   Single interface for all LLM calls.

&#x20;   No direct anthropic SDK calls outside this file.

&#x20;   

&#x20;   # TODO SCALE: replace SQLite counter with Redis INCR for atomic budget tracking under concurrent workers

&#x20;   """

&#x20;   

&#x20;   def \_\_init\_\_(self, settings: 'Settings', repository: 'Repository'):

&#x20;       """Initialize with config and repository for logging."""

&#x20;       ...

&#x20;   

&#x20;   def estimate\_tokens(self, text: str) -> int:

&#x20;       """Estimate tokens as int(len(text.split()) \* 1.3)."""

&#x20;       ...

&#x20;   

&#x20;   def check\_sonnet\_gates(

&#x20;       self,

&#x20;       narrative\_id: str,

&#x20;       ns\_score: float,

&#x20;       narrative\_created\_at: str,

&#x20;       estimated\_tokens: int

&#x20;   ) -> tuple\[bool, str]:

&#x20;       """

&#x20;       Check all 4 Sonnet gates in order.

&#x20;       Returns: (all\_passed, reason\_if\_failed)

&#x20;       """

&#x20;       ...

&#x20;   

&#x20;   def call\_haiku(

&#x20;       self,

&#x20;       task\_type: str,  # 'label\_narrative' | 'classify\_stage' | 'summarize\_mutation\_fallback'

&#x20;       narrative\_id: str,

&#x20;       prompt: str

&#x20;   ) -> str:

&#x20;       """

&#x20;       Execute Haiku call with retry logic and logging.

&#x20;       Falls back to defaults on failure.

&#x20;       """

&#x20;       ...

&#x20;   

&#x20;   def call\_sonnet(

&#x20;       self,

&#x20;       narrative\_id: str,

&#x20;       ns\_score: float,

&#x20;       narrative\_created\_at: str,

&#x20;       prompt: str

&#x20;   ) -> str | None:

&#x20;       """

&#x20;       Check gates, execute if passed.

&#x20;       Returns None if gates fail.

&#x20;       Falls back to Haiku on API failure.

&#x20;       """

&#x20;       ...

```



\### AssetMapper (asset\_mapper.py)



```python

class AssetMapper:

&#x20;   """

&#x20;   Maps narratives to financial assets via embedding similarity.

&#x20;   Zero LLM calls.

&#x20;   """

&#x20;   

&#x20;   def \_\_init\_\_(self, asset\_library\_path: str, embedder: 'EmbeddingModel'):

&#x20;       """

&#x20;       Load and validate asset library.

&#x20;       Raises FileNotFoundError if library not found.

&#x20;       Raises ValueError if dimension mismatch.

&#x20;       """

&#x20;       ...

&#x20;   

&#x20;   def map\_narrative(

&#x20;       self,

&#x20;       centroid: np.ndarray,

&#x20;       top\_k: int = 5,

&#x20;       min\_similarity: float = 0.50

&#x20;   ) -> list\[dict]:

&#x20;       """

&#x20;       Find matching assets for a narrative centroid.

&#x20;       Returns: \[{'ticker': str, 'asset\_name': str, 'similarity\_score': float}]

&#x20;       """

&#x20;       ...

```



\---



\## Database Schema



```sql

\-- narratives

CREATE TABLE narratives (

&#x20;   narrative\_id TEXT PRIMARY KEY,

&#x20;   name TEXT,

&#x20;   stage TEXT,

&#x20;   created\_at TEXT,

&#x20;   last\_updated\_at TEXT,

&#x20;   is\_coordinated INTEGER DEFAULT 0,

&#x20;   coordination\_flag\_count INTEGER DEFAULT 0,

&#x20;   suppressed INTEGER DEFAULT 0,

&#x20;   linked\_assets TEXT,  -- JSON

&#x20;   disclaimer TEXT,

&#x20;   human\_review\_required INTEGER DEFAULT 0,

&#x20;   is\_catalyst INTEGER DEFAULT 0,

&#x20;   document\_count INTEGER DEFAULT 0,

&#x20;   velocity REAL DEFAULT 0.0,

&#x20;   velocity\_windowed REAL DEFAULT 0.0,

&#x20;   centrality REAL DEFAULT 0.0,

&#x20;   entropy REAL,

&#x20;   intent\_weight REAL DEFAULT 0.0,

&#x20;   ns\_score REAL DEFAULT 0.0,

&#x20;   cohesion REAL DEFAULT 0.0,

&#x20;   polarization REAL DEFAULT 0.0,

&#x20;   cross\_source\_score REAL DEFAULT 0.0,

&#x20;   last\_assignment\_date TEXT,

&#x20;   consecutive\_declining\_days INTEGER DEFAULT 0

);



\-- centroid\_history

CREATE TABLE centroid\_history (

&#x20;   id INTEGER PRIMARY KEY AUTOINCREMENT,

&#x20;   narrative\_id TEXT,

&#x20;   date TEXT,

&#x20;   centroid\_blob BLOB

);



\-- candidate\_buffer

CREATE TABLE candidate\_buffer (

&#x20;   doc\_id TEXT PRIMARY KEY,

&#x20;   narrative\_id\_assigned TEXT,

&#x20;   embedding\_blob BLOB,

&#x20;   raw\_text\_hash TEXT,

&#x20;   source\_url TEXT,

&#x20;   source\_domain TEXT,

&#x20;   published\_at TEXT,

&#x20;   ingested\_at TEXT,

&#x20;   status TEXT DEFAULT 'pending',

&#x20;   raw\_text TEXT,

&#x20;   author TEXT

);



\-- llm\_audit\_log

CREATE TABLE llm\_audit\_log (

&#x20;   call\_id TEXT PRIMARY KEY,

&#x20;   narrative\_id TEXT,

&#x20;   model TEXT,

&#x20;   task\_type TEXT,

&#x20;   input\_tokens INTEGER,

&#x20;   output\_tokens INTEGER,

&#x20;   cost\_estimate\_usd REAL,

&#x20;   called\_at TEXT

);



\-- sonnet\_daily\_spend

CREATE TABLE sonnet\_daily\_spend (

&#x20;   date TEXT PRIMARY KEY,

&#x20;   total\_tokens\_used INTEGER DEFAULT 0,

&#x20;   total\_calls INTEGER DEFAULT 0

);



\-- adversarial\_log

CREATE TABLE adversarial\_log (

&#x20;   event\_id TEXT PRIMARY KEY,

&#x20;   narrative\_id TEXT,

&#x20;   detected\_at TEXT,

&#x20;   source\_domains TEXT,  -- JSON

&#x20;   similarity\_score REAL,

&#x20;   action\_taken TEXT

);



\-- robots\_cache

CREATE TABLE robots\_cache (

&#x20;   domain TEXT PRIMARY KEY,

&#x20;   rules\_text TEXT,

&#x20;   fetched\_at TEXT

);



\-- failed\_ingestion\_jobs

CREATE TABLE failed\_ingestion\_jobs (

&#x20;   job\_id TEXT PRIMARY KEY,

&#x20;   source\_url TEXT,

&#x20;   source\_type TEXT,

&#x20;   error\_message TEXT,

&#x20;   retry\_count INTEGER DEFAULT 0,

&#x20;   next\_retry\_at TEXT,

&#x20;   created\_at TEXT

);



\-- pipeline\_run\_log

CREATE TABLE pipeline\_run\_log (

&#x20;   run\_id TEXT PRIMARY KEY,

&#x20;   step\_number INTEGER,

&#x20;   step\_name TEXT,

&#x20;   status TEXT,

&#x20;   error\_message TEXT,

&#x20;   duration\_ms INTEGER,

&#x20;   run\_at TEXT

);



\-- narrative\_assignments

CREATE TABLE narrative\_assignments (

&#x20;   id INTEGER PRIMARY KEY AUTOINCREMENT,

&#x20;   narrative\_id TEXT,

&#x20;   doc\_id TEXT,

&#x20;   assigned\_at TEXT

);



\-- document\_evidence

CREATE TABLE document\_evidence (

&#x20;   doc\_id TEXT PRIMARY KEY,

&#x20;   narrative\_id TEXT,

&#x20;   source\_url TEXT,

&#x20;   source\_domain TEXT,

&#x20;   published\_at TEXT,

&#x20;   author TEXT,

&#x20;   excerpt TEXT

);

```



\---



\## Key Constants



\### LLM Pricing (llm\_client.py)



```python

\# Last updated: 2024-10. Update when Anthropic changes pricing.

\# Prices per 1M tokens in USD

HAIKU\_INPUT\_PRICE\_PER\_M = 0.80

HAIKU\_OUTPUT\_PRICE\_PER\_M = 4.00

SONNET\_INPUT\_PRICE\_PER\_M = 3.00

SONNET\_OUTPUT\_PRICE\_PER\_M = 15.00

```



\### Output Disclaimer (output.py)



```python

DISCLAIMER = "INTELLIGENCE ONLY — NOT FINANCIAL ADVICE. For informational purposes only."

```



\### Sentiment Lexicons (signals.py)



Define POSITIVE\_WORDS and NEGATIVE\_WORDS lists with at least 50 financially-relevant words each.



\### Fiscal Intent Vocabulary (signals.py)



```python

FISCAL\_INTENT\_VOCAB = \[

&#x20;   "allocating", "capex", "contracted", "committed",

&#x20;   "executing", "deployed", "acquiring", "divesting"

]



HEDGE\_VOCAB = \[

&#x20;   "potential", "possible", "could", "speculative",

&#x20;   "rumored", "considering", "exploring"

]

```



\---



\## Signal Formulas



\### Velocity

```

V = magnitude(C\_today − C\_yesterday) / (magnitude(C\_yesterday) + 1e-9)

```



\### Entropy

```

H = −Σ p(x) log p(x)

```

Over distribution of extracted entities. Return None if entity count < ENTROPY\_VOCAB\_WINDOW.



\### Intent Weight

```

intent\_ratio = fiscal\_matches / (hedge\_matches + fiscal\_matches + 1e-9)

```



\### Ns Score

```

Ns = (0.25 × velocity\_normalized × intent\_weight)

&#x20;  + (0.20 × cross\_source\_score)

&#x20;  + (0.15 × cohesion)

&#x20;  + (0.15 × polarization\_normalized)

&#x20;  + (0.15 × centrality)

&#x20;  + (0.10 × entropy\_normalized)

```



Normalization:

\- velocity\_normalized = min(velocity / 0.5, 1.0)

\- polarization\_normalized = min(polarization / 0.5, 1.0)

\- entropy\_normalized = entropy / log(ENTROPY\_VOCAB\_WINDOW) if not None else 0.0



\---



\## Sonnet Escalation Gates



All four must pass (check in order 1→4):



1\. `ns\_score > CONFIDENCE\_ESCALATION\_THRESHOLD`

2\. Narrative age >= 2 days (created\_at vs current date)

3\. No Sonnet call for this narrative\_id in last 24 hours

4\. `estimated\_tokens + daily\_spend < SONNET\_DAILY\_TOKEN\_BUDGET`



If gate 4 fails: log BUDGET\_CEILING\_HIT, fall back to Haiku.



\---



\## Pipeline Steps (Strict Order)



```

0\.  First-run initialization \& consistency checks

1\.  Log budget status

2\.  Retry failed ingestion jobs

3\.  Ingest raw documents

4\.  (robots.txt handled in ingesters)

5\.  LSH deduplication

6\.  Embed surviving documents

7\.  Assign to narratives or candidate\_buffer

8\.  Centroid decay for inactive narratives

9\.  Run HDBSCAN if buffer >= threshold

10\. Compute signals (Velocity, Entropy, Intent Weight)

11\. Run network centrality

12\. Compute Ns scores

13\. Apply adversarial filter

14\. Dispatch Haiku calls

15\. Check Sonnet gates, dispatch if passed

16\. Update sonnet\_daily\_spend

17\. Persist FAISS and LSH indices

18\. Write narrative state to DB

19\. Emit JSON output

20\. Cleanup old buffer entries

```



\---



\## Edge Case Handling



| Scenario | Behavior |

|----------|----------|

| Zero documents ingested | Log INFO, emit existing narratives, continue |

| All documents are duplicates | Log INFO, emit existing narratives, continue |

| Candidate buffer < threshold | Log INFO, skip clustering, documents wait |

| HDBSCAN finds no clusters | Log INFO, documents remain pending |

| No narratives exist | Emit empty JSON array `\[]` |

| Velocity requires history (day 1) | Return 0.0 |

| Entropy below threshold | Return None, use 0.0 in Ns |

| Centrality with < 2 narratives | Set centrality=0.0 for all |

| Sonnet budget exhausted | Fall back to Haiku |

| LLM call fails | Retry Haiku 2x, then use fallback text |

| Asset library missing | FATAL, halt pipeline |

| FAISS dimension mismatch | FATAL, halt pipeline |

| LSH pickle corrupted | Delete, reinitialize, WARNING |

| DB-FAISS count mismatch > 10% | WARNING, continue |



\---



\## Compliance Requirements



1\. \*\*robots.txt\*\*: Check before every HTTP request

2\. \*\*Source attribution\*\*: Every RawDocument must have source\_url, source\_domain, published\_at

3\. \*\*Disclaimer\*\*: Hardcoded, present on every output object, never loaded from DB

4\. \*\*LLM audit\*\*: Every call logged with tokens and cost



\---



\## Module Docstrings (Required)



\### robots.py

```python

"""

robots.txt compliance is a technical floor, not a legal guarantee.

Operators must independently review the Terms of Service of each target site

before enabling ingestion. This module enforces disallow rules but does not

constitute legal clearance.

"""

```



\### ingester.py

```python

"""

This system ingests only publicly available data. Source attribution metadata

is mandatory on all output objects for Fair Use compliance. Do not strip attribution.

"""

```



\### output.py

```python

"""

The disclaimer field is mandatory and immutable on all output objects.

This system provides intelligence signals only and must never be used to

generate financial advice, buy/sell recommendations, or price targets.

"""

```



\### pipeline.py

```python

"""

This pipeline operates under the assumption that all data sources have been

reviewed for Terms of Service compliance by the operator. The system logs

all source URLs for audit purposes.

"""

```



\### llm\_client.py

```python

"""

All LLM calls are logged with full token counts and cost estimates for budget

tracking and audit. Model outputs are used for analysis only and do not

constitute advice.

"""

```



\---



\## Scalability TODOs (Place Exactly as Shown)



```python

\# vector\_store.py - at class level

\# TODO SCALE: swap FaissVectorStore for PgVectorStore or PineconeVectorStore when moving to AWS



\# repository.py - at class level

\# TODO SCALE: swap SqliteRepository for PostgresRepository (psycopg2) on AWS RDS — interface is identical



\# llm\_client.py - at sonnet\_daily\_spend update

\# TODO SCALE: replace SQLite counter with Redis INCR for atomic budget tracking under concurrent workers



\# centrality.py - at compute\_centrality function

\# TODO SCALE: replace betweenness\_centrality with approximate harmonic centrality + sampling when active narrative count > 500



\# ingester.py - at PlaywrightIngester class

\# TODO: implement after MVP validation and legal ToS review per target site



\# deduplicator.py - at class level

\# TODO SCALE: replace pickle persistence with Redis backend for MinHashLSH when moving to multi-worker deployment

```



\---



\## Phase 5 - Orchestration Layer - COMPLETE

### pipeline.py

**Function:** `run() -> None`
Main orchestration function executing a full pipeline cycle in 20 strict steps (0–20).
Each step is wrapped in try/except and writes to `pipeline_run_log` with step_number, status, duration_ms.
FATAL errors (missing asset library, FAISS dimension mismatch) halt the pipeline immediately.

**Step summary:**
- Step 0: Initialize repository, validate asset library, load embedder + VectorStore + LSH, consistency checks
- Step 1: Log Sonnet daily budget; initialize spend record if absent
- Step 2: Retry failed ingestion jobs (RSS only for MVP)
- Step 3: Ingest via all active Ingester implementations
- Step 4: Implicit — robots.txt handled inside each Ingester
- Step 5: LSH deduplication; if zero survivors skip steps 6–7 (continue from step 8)
- Step 6: Embed surviving documents (L2-normalize)
- Step 7: Assign documents to narratives (momentum centroid update) or buffer as candidates
- Step 8: Centroid decay for narratives with zero new assignments this cycle
- Step 9: HDBSCAN clustering when candidate_buffer >= NOISE_BUFFER_THRESHOLD
- Step 10: Compute velocity, entropy, intent weight, cross-source score, cohesion, polarization
- Step 11: Build narrative graph, compute betweenness centrality, flag catalysts
- Step 12: Compute Ns score for all active narratives (None components default to 0.0)
- Step 13: Adversarial coordination check; apply −0.25 Ns penalty to coordinated narratives
- Step 14: Haiku labeling (all new narratives + unnamed below escalation threshold); lifecycle classification; noise eviction
- Step 15: Sonnet escalation (mutation analysis) for narratives passing all 4 gates
- Step 16: Sonnet daily spend managed inside LlmClient.call_sonnet()
- Step 17: Persist FAISS and LSH indices to disk
- Step 18: Finalize narrative state in DB
- Step 19: Build and emit structured output objects (validate_output + write_outputs)
- Step 20: Delete old candidate_buffer entries (status=clustered, older than 7 days)

**Helper functions:**
- `_log_step(repository, step_number, step_name, status, duration_ms, error_message)` — best-effort, never raises
- `_classify_lifecycle(narrative, centroid_history_vecs, today, velocity_window_days) -> str` — Declining/Emerging/Growing/Mature (first-match rule order)
- `_load_centroid_history_vecs(repository, narrative_id, days, emb_dim) -> list[np.ndarray]`

**Lifecycle classification rules (first match wins):**
1. Declining: velocity_windowed < 0 OR (|velocity_windowed| < 0.01 AND days_since_last_assignment > 3)
2. Emerging: velocity_windowed > 0 AND narrative_age <= 3 days AND document_count < 50
3. Growing: velocity_windowed > 0 AND document_count >= 50 AND velocity_windowed > prior_week_avg
4. Mature: default

**Noise eviction:** stage=Declining AND consecutive_declining_days > 14 AND ns_score < 0.20 → suppressed=1, deleted from FAISS index

### build_asset_library.py

**Function:** `build(download_dir=None) -> None`
Offline script. Downloads 10-K filings via sec-edgar-downloader for ~200+ major US tickers.
Extracts Item 1 Business section (first 2000 words), embeds with MiniLMEmbedder, L2-normalizes.
Saves pickle: `{ticker: {"name": str, "embedding": np.ndarray}}` to ASSET_LIBRARY_PATH.

**Helper functions:**
- `_extract_item1(text, max_words=2000) -> str` — extracts Item 1 Business section via regex; falls back to first 2000 words
- `_find_filing_text(ticker_dir) -> str` — walks sec-edgar-downloader output tree; prefers primary-document.txt

**Error handling:** individual ticker failures log WARNING/ERROR and continue; final summary logs total processed vs. failed.

### Deviations from ARCHITECTURE.md
-  not extracted as a named helper; Downloader import is inside  to guard against missing dependency
- Cohesion/polarization in Step 10 computed only from current-cycle doc embeddings for narratives that received new assignments; existing value retained otherwise (avoids re-embedding all historical docs at each cycle)

---

## Phase 4 - LLM & Output Layer - COMPLETE



\### Files Created

\- `narrative\_engine/llm\_client.py`

\- `narrative\_engine/asset\_mapper.py`

\- `narrative\_engine/output.py`



\### Classes \& Key Functions



\*\*llm\_client.py\*\*

\- Pricing constants: `HAIKU\_INPUT\_PRICE\_PER\_M = 0.80`, `HAIKU\_OUTPUT\_PRICE\_PER\_M = 4.00`, `SONNET\_INPUT\_PRICE\_PER\_M = 3.00`, `SONNET\_OUTPUT\_PRICE\_PER\_M = 15.00`

\- `class LlmClient`

&#x20; \- `def \_\_init\_\_(self, settings: Settings, repository: Repository) -> None`

&#x20; \- `def estimate\_tokens(self, text: str) -> int` — `int(len(text.split()) * 1.3)`

&#x20; \- `def check\_sonnet\_gates(self, narrative\_id: str, narrative\_created\_at: str, estimated\_tokens: int) -> tuple\[bool, str]`

&#x20; \- `def call\_haiku(self, task\_type: str, narrative\_id: str, prompt: str) -> str`

&#x20; \- `def call\_sonnet(self, narrative\_id: str, prompt: str) -> str | None`

&#x20; \- `def \_log\_pipeline\_error(self, step\_name: str, error\_message: str, status: str = "ERROR") -> None`



\*\*asset\_mapper.py\*\*

\- `class AssetMapper`

&#x20; \- `def \_\_init\_\_(self, asset\_library\_path: str, embedder: EmbeddingModel) -> None`

&#x20; \- `def map\_narrative(self, centroid: np.ndarray, top\_k: int = 5, min\_similarity: float = 0.50) -> list\[dict]`



\*\*output.py\*\*

\- `DISCLAIMER = "INTELLIGENCE ONLY — NOT FINANCIAL ADVICE. For informational purposes only."`

\- `def build\_output\_object(narrative, linked\_assets, supporting\_evidence, lifecycle\_reasoning, mutation\_analysis, score\_components) -> dict`

\- `def validate\_output(output: dict) -> bool`

\- `def write\_outputs(outputs: list\[dict], date: str) -> None`



\### Deviations from ARCHITECTURE.md

\- `check\_sonnet\_gates` signature omits `ns\_score: float` (present in the ARCHITECTURE.md interface). ns\_score is fetched from the repository inside the method. This keeps `call\_sonnet(narrative\_id, prompt)` clean and avoids callers needing to pre-fetch the narrative record.

\- `call\_sonnet` signature omits `ns\_score: float` and `narrative\_created\_at: str` (present in ARCHITECTURE.md). Both are fetched from the repository inside the method via `get\_narrative(narrative\_id)`.



\### Implementation Notes

\- `call\_sonnet` returns `None` when gates 1–3 fail (narrative not eligible). Gate 4 (budget ceiling) is handled differently: logs `BUDGET\_CEILING\_HIT` to `pipeline\_run\_log` and falls back to `call\_haiku("summarize\_mutation\_fallback", ...)`.

\- `call\_haiku` retry: 3 attempts total (initial + 2 retries) with exponential backoff 1s, 3s. On total failure uses `\_HAIKU\_FALLBACKS` dict: `label\_narrative → "Unlabeled Narrative"`, `classify\_stage → "Emerging"`, `summarize\_mutation\_fallback → "Analysis unavailable"`.

\- Sonnet call failure (after passing all 4 gates): logs ERROR, does NOT retry Sonnet (budget preserved), falls back to `call\_haiku("summarize\_mutation\_fallback", ...)`.

\- `update\_sonnet\_daily\_spend` called only on successful Sonnet API response; TODO SCALE comment placed immediately before that call.

\- `AssetMapper` builds a FAISS `IndexFlatIP` in memory on init from the pickled asset library dict `{ticker: {"name": str, "embedding": np.ndarray}}`. Dimension validated against `embedder.dimension()` on the first asset entry; raises `ValueError` on mismatch.

\- `AssetMapper.map\_narrative` returns results in descending similarity order, filtered to `>= min\_similarity`. Empty library returns `\[]`.

\- `build\_output\_object` always injects `DISCLAIMER` from the module constant — never from the narrative record or caller.

\- `validate\_output` checks 3 criteria in order: (1) disclaimer present and exact, (2) domains non-empty when supporting\_evidence non-empty, (3) narrative\_id is a valid UUID. Logs ERROR and returns False on failure; pipeline excludes that narrative without crashing.

\- `write\_outputs` creates `./data/outputs/{date}/` on first call. Emits to both file and stdout. Emits `\[]` for zero active narratives and logs INFO.

\- `output.py` uses `json.dumps(..., default=str)` to safely serialize any non-JSON-native field types.



---



\## Phase 3 - Processing Layer - COMPLETE



\### Files Created

\- `narrative\_engine/clustering.py`

\- `narrative\_engine/signals.py`

\- `narrative\_engine/centrality.py`

\- `narrative\_engine/adversarial.py`



\### Classes \& Key Functions



\*\*clustering.py\*\*

\- `def run\_clustering(repository: Repository, vector\_store: VectorStore, embedder: EmbeddingModel, settings: Settings) -> list\[str]`

\- Module constants: `\_MIN\_CLUSTER\_SIZE = 5`, `\_MIN\_SAMPLES = 3`



\*\*signals.py\*\*

\- `def compute\_velocity(centroid\_today: np.ndarray, centroid\_yesterday: np.ndarray) -> float`

\- `def compute\_velocity\_windowed(centroid\_history: list\[np.ndarray], window\_days: int) -> float`

\- `def compute\_entropy(documents: list\[str], min\_vocab\_size: int) -> float | None`

\- `def compute\_intent\_weight(documents: list\[str]) -> float`

\- `def compute\_cross\_source\_score(narrative\_domains: list\[str], corpus\_domain\_count: int) -> float`

\- `def compute\_cohesion(embeddings: list\[np.ndarray]) -> float`

\- `def compute\_polarization(documents: list\[str]) -> float`

\- `def compute\_ns\_score(velocity, intent\_weight, cross\_source\_score, cohesion, polarization, centrality, entropy, entropy\_vocab\_window: int = 10) -> float`

\- Module constants: `POSITIVE\_WORDS` (54 words), `NEGATIVE\_WORDS` (54 words), `FISCAL\_INTENT\_VOCAB`, `HEDGE\_VOCAB`



\*\*centrality.py\*\*

\- `def build\_narrative\_graph(narratives: list\[dict], vector\_store: VectorStore, similarity\_threshold: float = 0.40) -> nx.Graph`

\- `def compute\_centrality(graph: nx.Graph) -> dict\[str, float]`

\- `def flag\_catalysts(centrality\_scores: dict\[str, float]) -> list\[str]`



\*\*adversarial.py\*\*

\- `@dataclass class AdversarialEvent`

&#x20; \- Fields: `event\_id: str`, `affected\_narrative\_ids: list\[str]`, `source\_domains: list\[str]`, `similarity\_score: float`, `detected\_at: str`

\- `def check\_coordination(batch\_documents: list\[RawDocument], deduplicator: Deduplicator, trusted\_domains: list\[str], settings: Settings, repository: Repository) -> list\[AdversarialEvent]`



\### Deviations from ARCHITECTURE.md

\- `compute\_ns\_score` accepts `entropy\_vocab\_window: int = 10` as an explicit parameter (default matches `Settings.ENTROPY\_VOCAB\_WINDOW`). This keeps `signals.py` free of module dependencies as specified in the cross-module dependency graph, while still allowing the pipeline to inject the configured value.



\### Implementation Notes

\- `run\_clustering` deserializes `embedding\_blob` from `candidate\_buffer` as raw `float32` bytes via `np.frombuffer(blob, dtype=np.float32)` — pipeline must serialize embeddings with `ndarray.tobytes()`.

\- `run\_clustering` samples up to 500 existing narrative centroids from VectorStore as density context for HDBSCAN; first-run (empty VectorStore) clusters pending documents only.

\- `run\_clustering` calls `vector\_store.initialize(dim)` when `count() == 0` before the first centroid `add()` — guards against uninitialized index on the very first pipeline run.

\- `centroid\_history` blobs are stored as raw `float32` bytes (same convention as `embedding\_blob`) — pipeline must deserialize with `np.frombuffer(..., dtype=np.float32).reshape(-1)`.

\- `compute\_entropy` extracts ticker symbols via `\b\[A-Z]{1,5}\b` and fiscal/financial vocabulary terms; returns `None` when unique entity count < `min\_vocab\_size`.

\- `compute\_cohesion` uses matrix dot product on L2-normalized vectors (cosine similarity = dot product for unit vectors); takes upper triangle of the similarity matrix.

\- `check\_coordination` uses union-find single-linkage on pairs with Jaccard >= `LSH\_THRESHOLD`; checks `SYNC\_BURST\_MIN\_SOURCES` distinct non-trusted domains and `SYNC\_BURST\_WINDOW\_SECONDS` time span per cluster.

\- `check\_coordination` applies `−0.25` Ns penalty via `is\_coordinated` flag (read by pipeline at output step); does not mutate `ns\_score` directly — the pipeline applies the penalty at step 13.

\- `build\_narrative\_graph` uses networkx; `compute\_centrality` applies betweenness centrality then normalizes to \[0, 1] by dividing by the max score. TODO SCALE comment placed exactly as specified.



---



\## Phase 2 - Ingestion Layer - COMPLETE



\### Files Created

\- `narrative\_engine/robots.py`

\- `narrative\_engine/ingester.py`

\- `narrative\_engine/deduplicator.py`



\### Classes \& Key Functions



\*\*robots.py\*\*

\- `def can\_fetch(url: str, repository: Repository) -> bool`

\- Module-level constants: `\_USER\_AGENT = "NarrativeIntelligenceBot/1.0"`, `\_TTL\_HOURS = 24`



\*\*ingester.py\*\*

\- `@dataclass class RawDocument`

&#x20; \- Fields: `doc\_id: str`, `raw\_text: str`, `source\_url: str`, `source\_domain: str`, `published\_at: str`, `ingested\_at: str`, `author: str | None = None`, `raw\_text\_hash: str = ""`

\- `class Ingester(ABC)`

&#x20; \- `def ingest(self) -> list\[RawDocument]`

\- `class RssIngester(Ingester)`

&#x20; \- `def \_\_init\_\_(self, repository: Repository, feed\_urls: list\[str] | None = None) -> None`

&#x20; \- `def ingest(self) -> list\[RawDocument]`

\- `class EdgarIngester(Ingester)`

&#x20; \- `def \_\_init\_\_(self, repository: Repository, tickers: list\[str], forms: list\[str] | None = None, company\_name: str, email: str, download\_dir: str) -> None`

&#x20; \- `def ingest(self) -> list\[RawDocument]`

&#x20; \- `def \_read\_filings(self, ticker: str, form: str, ingested\_at: str) -> list\[RawDocument]`

\- `class PlaywrightIngester(Ingester)`

&#x20; \- `def ingest(self) -> list\[RawDocument]` — raises `NotImplementedError`

\- Module helpers: `\_compute\_hash`, `\_extract\_domain`, `\_backoff\_seconds`, `\_log\_failed\_job`, `\_parse\_published\_at`, `\_entry\_text`



\*\*deduplicator.py\*\*

\- `class Deduplicator`

&#x20; \- `def \_\_init\_\_(self, threshold: float, num\_perm: int, lsh\_path: str) -> None`

&#x20; \- `def load(self) -> bool`

&#x20; \- `def save(self) -> None`

&#x20; \- `def is\_duplicate(self, doc: RawDocument) -> bool`

&#x20; \- `def add(self, doc: RawDocument) -> None`

&#x20; \- `def get\_signature(self, doc: RawDocument) -> MinHash`

&#x20; \- `def get\_batch\_signatures(self) -> dict\[str, MinHash]`

&#x20; \- `def clear\_batch(self) -> None`

\- Module helper: `def \_extract\_shingles(text: str) -> set\[str]`



\### Deviations from ARCHITECTURE.md

None



\### Implementation Notes

\- `can\_fetch()` fetches robots.txt raw text via `urllib.request`, parses with `RobotFileParser.parse()` — single HTTP call covers both cache storage and rule parsing

\- `RssIngester` processes bozo feeds with partial entries (log warning); bozo feeds with zero entries are written to `failed\_ingestion\_jobs`

\- `EdgarIngester` handles both v4 (`Downloader(name, email, path)`) and v5+ (`Downloader(name, email)`) sec-edgar-downloader APIs via `TypeError` fallback

\- `EdgarIngester.\_read\_filings()` prefers `primary-document.txt` within each submission dir; falls back to any `.txt` file

\- `\_log\_failed\_job()` sets `next\_retry\_at = None` when `retry\_count >= 3` to halt retries; otherwise uses `min(300, 60 \* 2^retry\_count)` seconds

\- `Deduplicator.load()` handles corrupted pickle by deleting and reinitializing — implements the "LSH pickle corrupted → Delete, reinitialize, WARNING" edge case from this file

\- `Deduplicator.is\_duplicate()` wraps `\_lsh.query()` in try/except to handle empty-index edge case without raising

\- `Deduplicator.add()` catches `ValueError` from duplicate insert for idempotent behavior

\- `\_extract\_shingles()` falls back to `{text.lower()}` when text has fewer than 3 words

