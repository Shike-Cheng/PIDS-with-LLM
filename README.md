# PIDS-with-LLM

## Anomaly Detection with LLM. 

### The system can detect anomaly with LLM by using retrieval augmented generation (RAG) and reconstruct attack graphs based on anomaly detection.

#### Dataset preparation

> DARPA TC dataset

Download the DARPA TC dataset from: https://drive.google.com/drive/folders/1fOCY3ERsEmXmvDekG-LUUSjfWs6TRdp-

Setting Database: database.md

#### Create Database

```sh
python create_database.py
```

#### Graph Reduction

```sh
python getdata_from_database.py
python get_node_msg.py
```

#### Graph Reduction

```sh
python zip_edge.py
```

#### Knowledge Database

```sh
python make_benign_database.py
```

#### Suspicious Nodes and Rare Paths Selection

```sh
python main.py
```

#### Benign Knowledge Database

```sh
python select_path.py
```

####  Anomaly Detection with LLM

```sh
python openai_test.py
```

#### Attack Graph Reconstruction

```sh
python reconstruct_attack_graph.py
```


