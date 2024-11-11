hp:
	@echo " Compile hp_main ...";
	rm -f data.db build/ht_main
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/hp_main.c ./src/record.c ./src/hp_file.c -lbf -o ./build/hp_main -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c ./src/record.c -lbf -o ./build/bf_main -O2;

ht:
	@echo " Compile ht_main ...";
	rm -f data.db build/ht_main
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/record.c ./src/ht_table.c -lbf -o ./build/ht_main -O2

sht:
	@echo " Compile sht_main ...";
	rm -f data.db index.db build/sht_main
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sht_main.c ./src/record.c ./src/sht_table.c ./src/ht_table.c -lbf -o ./build/sht_main -O2

statistics:
	@echo " Compile statistics ...";
	rm -f data.db index.db build/statistics
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/statistics_main.c ./src/record.c ./src/sht_table.c ./src/ht_table.c ./src/statistics.c -lbf -o ./build/statistics -O2

clean:
	@echo " Cleaning up ...";
	rm -f block_example.db data.db index.db build/bf_main build/ht_main build/hp_main build/sht_main build/statistics

run_hp:
	@echo " Run hp_main ...";
	./build/hp_main

run_ht:
	@echo " Run ht_main ...";
	./build/ht_main

run_sht:
	@echo " Run sht_main ...";
	./build/sht_main

run_statistics:
	@echo " Run statistics ...";
	./build/statistics