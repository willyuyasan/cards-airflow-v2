#!/usr/bin/env python3
# Author: cdevito
# Description: Replay a specific jinja template with a specific replay JSON object from Cookiecutter
#              Created this as a bandage until I can work on full replay support in the templates

import json
import sys

try:
    from jinja2 import Template
except ImportError:
    print("Jinja2 not installed. Install it with pip3 install --user jinja2")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <JINJA_TEMPLATE> <COOKIECUTTER_REPLAY> > output")
        sys.exit(1)

    template_file = sys.argv[1]
    replay_file = sys.argv[2]

    with open(replay_file, "r") as f:
        template_data = json.loads(f.read())

    with open(template_file, "r") as f:
        template = Template(f.read())

    print(template.render(**template_data))
