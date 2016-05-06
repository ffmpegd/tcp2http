all:
	gcc -Wall -o tcp2http tcp2http.c map.c utils.c -I../../../3rdparty/include -L../../../3rdparty/lib -lavformat -lavdevice -lavcodec -lavfilter -lswresample -lswscale -lpostproc -lavutil -lx264 -lx265 -lfaac -lfreetype -lpthread

test: test.c map.c
	gcc -Wall -o test test.c map.c

clean:
	rm -rf tcp2http test *.o
