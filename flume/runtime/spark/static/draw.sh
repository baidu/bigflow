#!/bin/bash

for file in dot/*.dot; do
    dot -Tsvg -o ${file%.dot}.svg $file 2> /dev/null
done

for file in monitor/*.dot; do
    dot -Tsvg -o ${file%.dot}.svg $file 2> /dev/null
done
