open Future
module PSeq = Sequence.Seq(PFuture)(struct let use_mpi = true end)
(*module PSeq = Sequence.ListSeq*)

(* inverted_index computes an inverted index for the contents of
 * a given file. The filename is the given string.
 * The results are output to stdout. *)
open Sequence
open Util
       

let docs_to_word_list (doc: document): (int * string PSeq.t) =
  let words = PSeq.seq_of_array (Array.of_list (split_words doc.contents)) in
  let lower_words = PSeq.map (fun w -> String.lowercase w) words in
  (doc.id, lower_words)
;;

let filter (l: 'a PSeq.t) (f: 'a -> bool): 'a PSeq.t =
  let l_array = PSeq.array_of_seq l in
  let bits = PSeq.map (fun x -> if f x then 1 else 0) l in
  let bits_array = PSeq.array_of_seq bits in
  let length = PSeq.reduce (fun sum x -> sum + x) 0 bits in
  let bitsum = PSeq.scan (fun prev curr -> prev + curr) 0 bits in
  let bitsum_array = PSeq.array_of_seq bitsum in
  let filteredArray = Array.make length (PSeq.nth l 0) in
  let _ = PSeq.tabulate (fun i ->
    if bits_array.(i) == 0 then i
    else (Array.set filteredArray (bitsum_array.(i)-1) l_array.(i); i))
			(PSeq.length l) in
  PSeq.seq_of_array filteredArray
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

let get_words ((_, word_list): int * string PSeq.t)
	      ((id, doc_words): int * string PSeq.t) =
  let word_len = PSeq.length word_list in
  let doc_word_len = PSeq.length doc_words in
  let new_words = Array.make doc_word_len "" in
  let _ = PSeq.tabulate (fun i ->
    let word = PSeq.nth doc_words i in
    if find word_list word then ()
    else Array.set new_words i word) doc_word_len in
  let new_words' = filter (PSeq.seq_of_array new_words)
			  (fun x -> x <> "") in
  (0, PSeq.tabulate (fun i ->
    if i < word_len then
      PSeq.nth word_list i
    else
      PSeq.nth new_words' (i - word_len))
		((PSeq.length new_words') + word_len)) 
;;
  
let mkindex (args : string ) : unit =
  let docs = load_documents args in 
  let doc_words = PSeq.map docs_to_word_list
			   (PSeq.seq_of_array (Array.of_list docs)) in
  let (_, words) = PSeq.reduce get_words (0, PSeq.empty()) doc_words in 
  let results = PSeq.map (fun w ->
    let (_, ids) = PSeq.reduce (fun (_, id_list) (id, word_list) ->
      if find word_list w then
	let id_len = PSeq.length id_list in
	(0, PSeq.tabulate (fun i ->
	  if i < id_len then PSeq.nth id_list i 
	  else string_of_int id) (id_len + 1))
      else (0, id_list)) (0, PSeq.empty()) doc_words in
    (w, Array.to_list (PSeq.array_of_seq ids))) words in
  print_reduce_results (Array.to_list (PSeq.array_of_seq results))
;;

