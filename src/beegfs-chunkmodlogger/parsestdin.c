#include <stdio.h>
#include <string.h>
#include <unistd.h>


/* @return:
 *  1: Wrote entry to stdout
 *  0: Empty input. Done parsing string.
 * -1: Invalid entry or other i/o error.
 */
int parse_entry()
{
	int retval; //Return value from read(). -1 or 0 means we are done

	//First 8 bytes should be a unsigned long with a timestamp.
	char _time[8];

	//fgets(_time,sizeof(_time), stdin); //<- read the small-print. Reads at
					     //   most sizeof(_time)-1
	retval = read(stdin->_fileno, _time,sizeof(_time));

	if(retval == 0) //Done parsing. Return honky-donky.
		return 0;

	if(retval <= 0) // Malformed string or i/o error
		return -1;

	unsigned long time;
	memcpy(&time,_time,sizeof(_time));

	//Next 8 bytes should be a unsigned long with the lenght of the string
	//to come
	char _len[8];
	read(stdin->_fileno, _len,sizeof(_len));
	if(retval <= 0) // Malformed string or i/o error
		return -1;

	unsigned long len;
	memcpy(&len,_len,sizeof(_len));

	printf("%u ",time);
	printf("%u ",len);

	char path[len];
	read(stdin->_fileno, path,len);
	if(retval <= 0) // Malformed string or i/o error
		return -1;

	printf("%s\n",path);
	return 1;
}

/* Input will come from stdin.
 * - Assuming input is not malformed.
 * - Keep parsing until parse_entry() returns 0.
 */
int main(int argc, char **argv)
{

	int status;
	do{
		status = parse_entry();
	}
	while(status > 0);
}
