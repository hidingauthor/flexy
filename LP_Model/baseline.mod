///////////////////////////////////
// LP model in Baseline (PaaU)
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
int Q=...;            //number of Programs
range j=1..Q;
float c[j]=...;       //cost of Programs
int f[j]=...;         // frequency of Programs
{int} qi[s]=...;      //columns in independent Programs

float L=1/K;          //Balanced Workload cost

float C= sum(oj in j) f[oj]*c[oj]; //Workload cost

//Decision variable
 dvar boolean x[i][k]; //replica allocation configuration
 dvar boolean y[j][k]; //allowed routing
 dvar float z[j][k] in 0..1;  //routing configuration
 
 {int} mi=union(os in s:f[os]>0)qi[os];

//Objective Function
minimize sum(oi in i, ok in k) x[oi][ok]*a[oi]; 

//Contraints
subject to{  
     
     //Constraint for allocation configuration for possible EB routing
	 forall(oj in j, ok in k) y[oj][ok]*card(q[oj])<=sum(oi in q[oj])x[oi][ok];
        
     //Constraint for workload sharing in possible EB routing:
	 forall(oj in j, ok in k) z[oj][ok]<=y[oj][ok];      
   
     //Constraint for workload balancing
	 forall(ok in k) sum(oj in j) f[oj]*c[oj]/C*z[oj][ok] <= L;
       
     //Constraint for total workload sharing of all EB routing:
	 forall(oj in j) sum(ok in k) z[oj][ok]==1;    

  }

float W= sum(oi in i, ok in k) x[oi][ok]*a[oi];  //the total stored fragments for the routing configuration of EB, 𝐹_total
{int} mi=union(oj in j:f[oj]>0)q[oj];
float V= sum(oi in mi) a[oi];                    //the minimum total stored fragments for all EBs, 𝐹min
float RF=W/V;                                    //Replication Factor (RF)

execute{    
  writeln(W);
  writeln(V);
  writeln(RF);
   
  }