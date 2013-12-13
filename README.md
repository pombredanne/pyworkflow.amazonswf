# pyworkflow.amazonswf

## amazon swf backend for pyworkflow

[pyworkflow](http://github.com/pyworkflow/pyworkflow) supports the easy implementation of workflows, and handling the
execution of workflow processes, across multiple backends. This is the
backend for pyworkflow that provides integration with [Amazon Simple Workflow Framework](http://aws.amazon.com/swf/).

## Usage

AmazonSWFBackend supports integration of pyworkflow with Amazon's Simple
Workflow Framework service.

````python
from pyworkflow.amazonswf import AmazonSWFBackend
from pyworkflow.managed import Manager

backend = AmazonSWFBackend(ACCESS_KEY_ID, SECRET_ACCESS_KEY, region='us-east-1', domain='foo.bar')
manager = Manager(backend=backend)
````

## About

### License

pyworkflow.amazonswf is under the MIT License.

### Contact

pyworkflow.amazonswf is written by [Willem Bult](https://github.com/willembult).

Project Homepage: [https://github.com/pyworkflow/pyworkflow.amazonswf](https://github.com/pyworkflow/pyworkflow.amazonswf)

Feel free to contact me. But please file issues in github first. Thanks!
