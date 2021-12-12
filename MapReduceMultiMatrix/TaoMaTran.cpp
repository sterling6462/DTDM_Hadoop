
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char *argv[])
{
	
	 if( argc <=2 )
   {
      printf("Nhap vao tren dong lenh: <TenMaTran> m  n\n");
      return 0;
   }
   
//	int M = cStringToInt(argv[2]);
//	int N = cStringToInt(argv[3]);
	
	int M = atoi(argv[2]);
	int N = atoi(argv[3]);
	

int i,j,r;
for (i=0;i<M;i++)
for (j=0;j<N;j++)
{
	r = 20 + rand()%(50 + 1 - 20); // Sinh rand trong [20, 50]
	printf("%s,%d,%d,%d\n", argv[1], i, j, r);
}

	return 0;
}

