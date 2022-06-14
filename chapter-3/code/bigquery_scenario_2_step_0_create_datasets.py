# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

client = bigquery.Client()

datasets_name = ['raw_bikesharing','dwh_bikesharing','dm_bikesharing']
location = 'US'

def create_bigquery_dataset(dataset_name):
    """Create bigquery dataset. Check first if the dataset exists
        Args:
            dataset_name: String
    """

    dataset_id = "{}.{}".format(client.project, dataset_name)
    try:
        client.get_dataset(dataset_id)
        print("Dataset {} already exists".format(dataset_id))
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


for name in datasets_name:
    create_bigquery_dataset(name)
