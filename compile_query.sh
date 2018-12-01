#!/bin/bash

# Replacing parxer output with parameterized code
rm Code/$1.cpp
mv Code/temp.cpp Code/$1.cpp

g++ -std=c++11 -fPIC -c ./Code/$1.cpp -o ./Code/$1.o
g++ -shared -o ./Code/$1.so ./Code/$1.o
