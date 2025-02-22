# Copyright 2019 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging as _logging

# SET UP NULL LOG HANDLER
_logger = _logging.getLogger(__name__)
_logger.setLevel(_logging.DEBUG)
_logger.addHandler(_logging.NullHandler())

from .dag import (
    DAG,
    NodeLayer,
    SubDAG,
    DotConfig,
    NodeStatusFile,
    ManyToMany,
    OneToOne,
    Script,
    DAGAbortCondition,
    FinalNode,
    WalkOrder,
    Nodes,
)
from .writer import DEFAULT_DAG_FILE_NAME, CONFIG_FILE_NAME, SEPARATOR
from . import exceptions

from .version import __version__, version_info, version
