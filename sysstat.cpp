#include <Rcpp.h>
#include <string>
using namespace Rcpp;
using namespace std;

// This is a simple example of exporting a C++ function to R. You can
// // source this function into an R session using the Rcpp::sourceCpp 
// // function (or via the Source button on the editor toolbar). Learn
// // more about Rcpp at:
// //
// //   http://www.rcpp.org/
// //   http://adv-r.had.co.nz/Rcpp.html
// //   http://gallery.rcpp.org/
// //
//
// [[Rcpp::export]]
 NumericVector timesTwo(NumericVector x) {
   return x * 2;
   }

// [[Rcpp::export]]
 int getsize(NumericVector x) {
   return x.size();
   }

// [[Rcpp::export]]
 NumericVector getbytes(NumericVector x) {
   int N =  x.size();
   int i = 0;
   Rcpp::NumericVector rtn(N);
   for (i = 1; i < N; i++){
	rtn[i] = x[i] - x[i-1];
   }
   return rtn;
   }



// [[Rcpp::export]]
string hello(string name) {
  cout << "hello " << name << endl;  
  return name;
}

//
//
//   // You can include R code blocks in C++ files processed with sourceCpp
//   // (useful for testing and development). The R code will be automatically 
//   // run after the compilation.
//   //
//
//   /*** R
//   timesTwo(42)
//   */
//
