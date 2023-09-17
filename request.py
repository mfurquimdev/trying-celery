#!/usr/bin/env python

from tasks import add

import random

num = list(range(11))

for i in range(1):
    add.delay(random.choice(num),random.choice(num))

