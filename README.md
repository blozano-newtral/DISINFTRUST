# DISINFTRUST

`python3 -i src/clustering_main.py --input-dataset data/claims_parse_urls-6m-hard_clean.json --output-folder outputs`

Genera un fichero de output en `outputs/`:

`hr-cs-da-nl-en-et-fi-fr-de-el-hu-ga-it-lv-lt-mt-pl-pt-ro-sk-sl-es-sv_umap_10_hdbscan_sentence-transformers-paraphrase-multilingual-MiniLM-L12-v2.csv`

donde las columnas son los campos textuales usados para el experimento, más una columna `cluster_labels`.

