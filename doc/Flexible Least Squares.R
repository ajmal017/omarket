fls <-
function (A, b, mu=1, ncap=length(b))
{
  m <- nrow (A)
  n <- ncol (A)
  M <- array (0,c(n,n,ncap))
  E <- array (0,c(n,ncap))
  X <- array (0,c(n,ncap))
  R <- matrix(0,n,n)
  diag(R) <- diag(R) + mu
  for (j in 1:ncap) {
    Z <- solve(qr(R + tcrossprod(A[j,]),LAPACK=TRUE),diag(1.0,n));
    M[,,j] <- mu*Z             # (5.7b)
    v <- b[j]*A[j,]
    if(j==1) p <- rep(0,n) else p <- mu*E[,j-1]
    w <- p + v
    E[,j] <- Z %*% w           # (5.7c)
    R <- -mu*mu*Z
    diag(R) <- diag(R) + 2*mu
  }
# Calculate eqn (5.15) FLS estimate at ncap
  Q <- -mu*M[,,ncap-1]
  diag(Q) <- diag(Q) + mu
  Ancap <- A[ncap,,drop=FALSE]
  C <- Q + t(Ancap) %*% Ancap
  d <- mu*E[,ncap-1,drop=FALSE] + b[ncap]*t(Ancap)
  X[,ncap] <- C %*% d
  X[,ncap] <- solve(qr(C,LAPACK=TRUE),d)
  X <- X[,ncap]
  X
}