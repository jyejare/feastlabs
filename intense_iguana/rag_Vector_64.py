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
    ValueType
)
from feast.types import Float32, Float64, Int64, String, Array

project = Project(name="EDB_project_new", description="A project for documents retrieval for EDB")

#doc = Entity(name="item_id", join_keys=["item_id"])

item_id = 'item_id'
ENTITY_NAME_ITEM = item_id.split('_')[0]

item_entity = Entity(
    name=ENTITY_NAME_ITEM,
    join_keys=[item_id],
    value_type=ValueType.INT64,
    description=item_id.replace('_', ''),
)

item_embed_df = pd.read_parquet(path='data/pushed_item_embedding.parquet')

item_embed_push_fs = FileSource(
    name="document_embeddings_edb",
    path="data/pushed_item_embedding.parquet",
    timestamp_field="event_timestamp",
)

store = FeatureStore(repo_path=".")

store.apply(project, item_entity, item_embed_push_fs)

from feast.data_source import PushMode

item_embed_push_source = PushSource(name='item_embed_push_source_edb', batch_source=item_embed_push_fs)

store.apply(item_embed_push_source)

# item_embedding_view = FeatureView(
#     name="item_embedding_edb",
#     entities=[doc],
#     ttl=timedelta(days=1),
#     schema=[
#         Field(name="item_id", dtype=Int64),
#         Field(
#             name="embedding",
#             dtype=Array(Float32),
#             vector_index=True,
#         ),
#     ],
#     source=item_embed_push_source,
#     online=True,
# )

item_embedding_view = FeatureView(
    name="item_embedding",
    entities=[item_entity],
    ttl=timedelta(days=365 * 5),
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
    online=True
)

store.apply(item_embedding_view)

pushresult = store.push(push_source_name='item_embed_push_source_edb', df=item_embed_df, to=PushMode.ONLINE)

import numpy as np
for n in range(99):
    # user_embed = [0.15] * 64
    user_embed = list(np.random.randn(64))

    print(store.retrieve_online_documents(
        query=user_embed,
        top_k=64,
        features=[
            'item_embedding_edb:embedding', 'item_embedding_edb:item_id'
        ]
    ).to_df())
