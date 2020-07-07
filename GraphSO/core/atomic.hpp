#ifndef ATOMIC_H
#define ATOMIC_H

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

template <class ET>
inline bool cas(ET *ptr, ET oldv, ET newv) {       //在写入新值之前， 读出旧值， 当且仅当旧值与存储中的当前值一致时，才把新值写入存储。
	if (sizeof(ET) == 8) {
		return __sync_bool_compare_and_swap((long*)ptr, *((long*)&oldv), *((long*)&newv));
	} else if (sizeof(ET) == 4) {
		return __sync_bool_compare_and_swap((int*)ptr, *((int*)&oldv), *((int*)&newv));
	} else {
		assert(false);
	}
}

template <class ET>
inline bool write_min(ET *a, ET b) {
	ET c; bool r=0;
	do c = *a;
	while (c > b && !(r=cas(a,c,b)));
	return r;
}
template <class ET>
inline bool write_max(ET *a, ET b) {
	ET c; bool r=0;
	do c = *a;
	while (c < b && !(r=cas(a,c,b)));
	return r;
}

template <class ET>
inline void write_add(ET *a, ET b) {
	volatile ET newV, oldV;
	do {oldV = *a; newV = oldV + b;}
	while (!cas(a, oldV, newV));
}

#endif
