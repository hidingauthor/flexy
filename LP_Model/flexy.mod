///////////////////////////////////
// LP model in Flexy
///////////////////////////////////
// we only provide the model (.mod) file. 
// it is recommended to create the data (.dat) file.
////////////////////////////////////////////////////

//variables

int K=...;            //number of nodes
range k=1..K;
int N=...;            //number of fragment(columns)
range i=1..N;
float a[i]=...;       //fragment volume
int S=...;            //number of EBs
range s=1..S;
{int} r[s]=...;       //Producer EBs
int loop[s]=...;      //iteration of EBs
float c[s]=...;       //cost of EBs
float d[s]=...;       //data transfer costs of EBs
int f[s]=...;         // frequency of EBs
{int} qi[s]=...;      //columns in independent EBs

float L=1/K;          //balanced Workload cost

int R[s][s];          //relationship matrix


execute{  
  for(var gr in s){
    for(var gc in r[gr]){
      R[gr][gc]=1;      
      }    
    }  
  }
 
//Decision variable
 dvar boolean x[i][k]; //replica allocation configuration
 dvar boolean y[s][k]; //allowed routing
 dvar float+ z[s][k];  //routing configuration
 
 

//Objective Function
minimize sum(oi in i, ok in k) x[oi][ok]*a[oi]; 

//Contraints
subject to{  
     
     //Constraint for allocation configuration for possible EB routing
	 forall(os in s, ok in k) y[os][ok]*card(qi[os])<=sum(oi in qi[os])x[oi][ok];
        
     //Constraint for workload sharing in possible EB routing:
	 forall(os in s, ok in k) z[os][ok]<=y[os][ok];      
   
     //Constraint for workload balancing
	 forall(ok in k) sum(os in s) f[os]*(c[os]*loop[os]+ sum(ou in r[os]) (1-R[ou][os]*z[os][ok]*z[ou][ok]*d[ou])*loop[os]*loop[ou])/(sum(os in s) f[os]*(c[os]*loop[os]+ sum(ou in r[os]) (1-R[ou][os]*z[os][ok]*z[ou][ok]*d[ou])*loop[os]*loop[ou]))*z[os][ok] <= L;
       
     //Constraint for total workload sharing of all EB routing:
	 forall(os in s) sum(ok in k) z[os][ok]==1;    

  }

float W= sum(oi in i, ok in k) x[oi][ok]*a[oi];  //the total stored fragments for the routing configuration of EB, 𝐹_total
{int} mi=union(os in s:f[os]>0)qi[os];
float V= sum(oi in mi) a[oi];                    //the minimum total stored fragments for all EBs, 𝐹min
float RF=W/V;                                    //Replication Factor (RF)

execute{    
  writeln(W);
  writeln(V);
  writeln(RF);
   
  }
  