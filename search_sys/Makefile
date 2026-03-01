CXX = g++
CFLAGS = -O2 -std=c++17 -Wall -Wextra

all: bin/tok bin/idx bin/qry

bin/tok: cpp/tok_main.cpp cpp/porter.hpp cpp/vec.hpp
	@if not exist bin mkdir bin
	$(CXX) $(CFLAGS) -o bin/tok cpp/tok_main.cpp

bin/idx: cpp/idx_main.cpp cpp/vec.hpp cpp/map.hpp cpp/varint.hpp cpp/porter.hpp
	@if not exist bin mkdir bin
	$(CXX) $(CFLAGS) -o bin/idx cpp/idx_main.cpp

bin/qry: cpp/qry_main.cpp cpp/vec.hpp cpp/map.hpp cpp/varint.hpp cpp/porter.hpp
	@if not exist bin mkdir bin
	$(CXX) $(CFLAGS) -o bin/qry cpp/qry_main.cpp

clean:
	@if exist bin rmdir /s /q bin
