/* Generated By:JJTree: Do not edit this line. ASTUnary.java Version 6.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=true,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.netflix.sel.ast;

import com.netflix.sel.visitor.SelBaseNode;

public class ASTUnary extends SelBaseNode {
  public ASTUnary(int id) {
    super(id);
  }

  public ASTUnary(SelParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. * */
  public Object jjtAccept(SelParserVisitor visitor, Object data) {

    return visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=22128231493b6f887eba88be30051020 (do not edit this line) */