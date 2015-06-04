#!/bin/bash

CHUNK_DIR="/store01/chunks"

find $CHUNK_DIR -type f -printf "%T@\0%p\0%s"
