ht:
	@echo " Compile ht_main ...";
	g++ -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.cpp ./src/hash_file.cpp -lm -lbf -o ./build/runner -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./build/runner -O2

clean:
	rm -f data_*.db
	rm -f build/runner

run:
	make clean
	make
	./build/runner

debug:
	make clean
	make
	valgrind ./build/runner

