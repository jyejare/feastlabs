from datetime import timedelta, datetime

import pandas as pd

from feast import (
    FeatureStore,
    Entity,
    FeatureView,
    Field,
    FileSource,
    Project,
    PushSource,
)
from feast.types import Float32, Float64, Int64, String, Array

project = Project(name="intense_iguana", description="A project for documents retrieval")

doc = Entity(name="item_id", join_keys=["item_id"])

item_embed_df = pd.read_parquet(path='data/doc_embed.parquet')

item_embed_push_fs = FileSource(
    name="document_embeddings",
    path="data/doc_embed.parquet",
    timestamp_field="event_timestamp",
)

store = FeatureStore(repo_path=".")

store.apply(project, doc, item_embed_push_fs)

from feast.data_source import PushMode

item_embed_push_source = PushSource(name='item_embed_push_source', batch_source=item_embed_push_fs)

store.apply(item_embed_push_source)

item_embedding_view = FeatureView(
    name="item_embedding",
    entities=[doc],
    ttl=timedelta(days=1),
    schema=[
        Field(name="item_id", dtype=Int64),
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_search_metric="cosine",
        ),
    ],
    source=item_embed_push_source,
    online=True,
)


store.apply(item_embedding_view)

# pushresult = store.push(push_source_name='item_embed_push_source', df=item_embed_df, to=PushMode.ONLINE)

# store.apply(doc)

user_embed = [0.15] * 3

print(store.retrieve_online_documents(query=user_embed, top_k=10, features=['item_embedding:item_id', 'item_embedding:embedding', 'item_embedding:distance']).to_df())
