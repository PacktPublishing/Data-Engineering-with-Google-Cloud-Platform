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

def sum_two_values(arg_1:int , arg_2:int):
    result = arg_1 + arg_2
    return result

if __name__ == "__main__":
    VALUE_1 = 10
    VALUE_2 = 20

    RESULT_VALUE = sum_two_values(VALUE_1, VALUE_2)
    print(RESULT_VALUE)
