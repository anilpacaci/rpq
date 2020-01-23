#!/bin/bash

scripts/manual-runner.py conf/watdiv-sf250.json |& tee watdiv-250.run ;
scripts/manual-runner.py conf/watdiv-sf200.json |& tee watdiv-200.run ;
scripts/manual-runner.py conf/watdiv-sf150.json |& tee watdiv-150.run ;
scripts/manual-runner.py conf/watdiv-sf100.json |& tee watdiv-100.run ;
scripts/manual-runner.py conf/watdiv-sf50.json |& tee watdiv-50.run ;

