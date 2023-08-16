package io.klaytn.utils

trait LazyEvalUtil extends Serializable {
  implicit class LazyEvalSyntax[A](a: => A) {
    @inline def toLazyEval: LazyEval[A] = LazyEval(a _)
  }

  case class LazyEval[+A](getLazyInstance: () => A) extends Serializable {
    @inline
    private[LazyEvalUtil] def eval: A = getLazyInstance()
  }

  implicit def __toLazyEval[A](a: => A): LazyEval[A] = LazyEval(a _)
  implicit def __fromLazyEval[A](a: LazyEval[A]): A = a.eval
}

object LazyEvalUtil extends LazyEvalUtil
