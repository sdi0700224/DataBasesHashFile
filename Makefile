CXX = g++
CC = gcc
CXXFLAGS = -I./include/
LDFLAGS = -L./lib/ -Wl,-rpath=./lib/
LDLIBS = -lm -lbf
OUTPUT_DIR = ./build

.PHONY: all clean run debug

all: ht

ht: $(OUTPUT_DIR)/ht_main

$(OUTPUT_DIR)/ht_main: ./examples/ht_main.cpp ./src/hash_file.cpp | $(OUTPUT_DIR)
	@echo "Compiling ht_main..."
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $^ $(LDLIBS) -O2 -o $@

clean:
	@echo "Cleaning up..."
	rm -f data_*.db
	rm -rf $(OUTPUT_DIR)

run: all
	@echo "Running..."
	$(OUTPUT_DIR)/ht_main

debug: CXXFLAGS += -g
debug: CCFLAGS += -g
debug: all
	@echo "Debugging ht_main..."
	valgrind $(OUTPUT_DIR)/ht_main

$(OUTPUT_DIR):
	mkdir -p $(OUTPUT_DIR)
