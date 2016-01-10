open Future
(*module PSeq = Sequence.Seq(PFuture)(struct let use_mpi = true end)*)
module PSeq = Sequence.ListSeq

(* inverted_index computes an inverted index for the contents of
 * a given file. The filename is the given string.
 * The results are output to stdout. *)
open Sequence
open Util
       
(*	
let doc_get_words (doc: (string * string PSeq.t) PSeq.t) =
  (*or should it be 0*)
  let words = snd (PSeq.nth doc 0) in
  PSeq.map (fun w -> PSeq.singleton (w, PSeq.empty)) words
;;
  
let doc_get_id (doc: (string * string PSeq.t) PSeq.t) =
  (*or should it be 0*)
  fst (PSeq.nth doc 0)
;;

let docs_to_word_list (doc: document): (string * string PSeq.t) =
   PSeq.seq_of_array (Array.of_list (split_words doc.contents))
;;
let find (list: 'a PSeq.t) (elt: 'a): bool =
  match (PSeq.map_reduce (fun x -> Some x)
			(fun result opt ->
			 match opt with
			 | Some y -> if elt = y then
				       Some y
				     else result
			 | None -> failwith "should not reach") None list) with
  | Some _ -> true
  | None -> false
;;
  
let reduce_docs (word_list: string PSeq.t) (doc_list: string PSeq.t): string PSeq.t =
  PSeq.reduce (fun curr_word_list word ->
	       if find curr_word_list word then
		 curr_word_list
	       else
		 PSeq.cons curr_word_list word) word_list doc_list
 *)
let mkindex (args : string ) : unit =
()
(*  let docs = load_documents args in (*should we use map reduce to compute these?*)
  let doc_seq = PSeq.seq_of_array (Array.of_list docs) in
  let list_of_words = PSeq.map_reduce docs_to_word_list reduce_docs PSeq.empty docs_seq in
  let results = PSeq.map (fun a -> (fst a, Array.to_list PSeq.array_of_seq (snd a))) raw_results in
  print_reduce_results (Array.to_list PSeq.array_of_seq results)
 *);;

