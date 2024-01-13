#[allow(unused_imports)]
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "src/request_manager/lucene_grammar.pest"]
pub struct LuceneQueryParser;
