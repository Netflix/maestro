/* Generated By:JJTree: Do not edit this line. ASTImportDeclaration.java Version 6.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=true,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.netflix.sel.ast;

import com.netflix.sel.visitor.SelBaseNode;

public class ASTImportDeclaration extends SelBaseNode {
  public ASTImportDeclaration(int id) {
    super(id);
  }

  public ASTImportDeclaration(SelParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. * */
  public Object jjtAccept(SelParserVisitor visitor, Object data) {

    return visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=5fb18795e713806819acdb5ab5912c0f (do not edit this line) */
