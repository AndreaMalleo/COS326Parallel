open System
open Future 
open Mpi


module type S = sig
  type 'a t
  val tabulate : (int -> 'a) -> int -> 'a t
  val seq_of_array : 'a array -> 'a t
  val array_of_seq : 'a t -> 'a array
  val iter: ('a -> unit) -> 'a t -> unit
  val length : 'a t -> int
  val empty : unit  ->'a t
  val cons : 'a -> 'a t -> 'a t
  val singleton : 'a -> 'a t
  val append : 'a t -> 'a t -> 'a t
  val nth : 'a t -> int -> 'a
  val map : ('a -> 'b) -> 'a t -> 'b t
  val map_reduce : ('a -> 'b) -> ('b -> 'b -> 'b) -> 'b -> 'a t -> 'b
  val reduce : ('a -> 'a -> 'a) -> 'a -> 'a t -> 'a
  val flatten : 'a t t -> 'a t
  val repeat : 'a -> int -> 'a t
  val zip : ('a t * 'b t) -> ('a * 'b) t
  val split : 'a t -> int -> 'a t * 'a t
  val scan: ('a -> 'a -> 'a) -> 'a -> 'a t -> 'a t
end



(*******************************************************)
(* Sequential Sequences Based on a List Representation *)
(*******************************************************)

module ListSeq : S = struct

  type 'a t = 'a list

  let length = List.length

  let empty () = []

  let cons (x:'a) (s:'a t) = x::s

  let singleton x = [x]

  let append = List.append

  let tabulate f n =
    let rec helper acc x =
      if x = n then List.rev acc
      else helper ((f x)::acc) (x+1) in
    helper [] 0

  let nth = List.nth

  let filter = List.filter

  let map = List.map

  let reduce = List.fold_left

  let map_reduce m r b s = reduce r b (map m s)

  let repeat x n =
    let rec helper x n acc =
      if n = 0 then acc else helper x (n-1) (x::acc) in
    helper x n []

  let flatten = List.flatten

  let zip (s1,s2) = List.combine s1 s2

  let split s i =
    let rec helper s i acc =
      match s,i with
        | [],_ -> failwith "split"
        | _,0 -> (List.rev acc,s)
        | h::t,_ -> helper t (i-1) (h::acc) in
    helper s i []

  let iter = List.iter

  let array_of_seq = Array.of_list

  let seq_of_array = Array.to_list

  let scan f b s = 
    let (_,xs) = List.fold_left (fun (v,ls) e -> let r = f v e in (r,r::ls)) (b,[]) s in
    List.rev xs

end


(*******************************************************)
(* Parallel Sequences                                  *)
(*******************************************************)

module type SEQ_ARGS = sig 
  val use_mpi: bool
end

module Seq (Par : Future.S) (Arg : SEQ_ARGS) : S = struct

  type 'a t = 'a array


  let num_cores = System.cpu_count ()

  let num_chunks = 3

  let force_arrays (n: int) (a: 'a array Par.future array) (num_chunks: int) : 'a array = 
    Array.init n (fun i ->
		  let chunk_index = (n * i) / num_chunks in
		  let chunk = Par.force a.(chunk_index) in
		  chunk.(i-chunk_index))

  let tabulate f n = 
    let result = Array.init num_chunks (fun i ->
      let lo = (n * i) / num_chunks in
      let hi = (n * (i+1) / num_chunks) - 1 in
      Par.future (Array.init (hi - lo + 1))
	       (fun i -> f (i + lo))) in
    force_arrays n result num_chunks
  ;;
    
  let seq_of_array a = a


  let array_of_seq seq = seq


  let iter f seq = 
    let copyseq = Array.copy seq in
    Array.iter f copyseq


  let length seq = Array.length seq


  let empty () = seq_of_array [||]


  let cons elem seq = Array.init (length seq + 1) (fun i -> 
    if (i == 1) then elem
    else seq.(i-1))


  let singleton elem = Array.of_list [elem] 


  let append seq1 seq2 = Array.append seq1 seq2


  let nth seq i = seq.(i)

  let map f seq = 
    tabulate (fun i -> f seq.(i)) (length seq)
  ;;

  let map_reduce m r b seq =
    let n = length seq in  
    let result = Array.init num_chunks (fun i ->
     let lo = (n * i) / num_chunks in
     let hi = (n * (i+1) / num_chunks) in
     Par.future (fun x ->
		 let maped_array = Array.map m x in
		 Array.fold_left r (Array.get maped_array 1) (Array.sub maped_array 1 (hi-lo)))
		(Array.sub seq lo hi)) in  
    Array.fold_left (fun acc elt -> r acc (Par.force elt)) b result
 ;;
   
  (*order is acc elt*)
 let reduce f b seq =
   let n = length seq in
   let result = Array.init num_chunks (fun i ->
     let l = (n * i) / num_chunks in
     let r = (n * (i+1) / num_chunks) in
     Par.future (Array.fold_left f (Array.get seq l))
	      (Array.sub seq (l+1) r)) in 
    Array.fold_left (fun acc elt -> f acc (Par.force elt)) b result
  ;;

  let flatten seqseq = 
    Array.fold_left (fun acc b -> Array.append acc b) (empty ()) (array_of_seq seqseq)
      
  let repeat elem num = Array.make num elem


  (*check for lengths*)
  let zip (seq1,seq2) = 
    let l1 = length seq1 in
    let l2 = length seq2 in
    if (l1 < l2) then  
      Array.init l1 (fun i-> (seq1.(i),seq2.(i))) 
    else 
     Array.init l2 (fun i-> (seq1.(i), seq2.(i))) 
    


  let split seq x = 
    if x > length seq then
      failwith "index beyond the limit of the sequence"
   else 
      let fst = Array.sub seq 0 (x-1) in
      let snd = Array.sub seq x (length seq) in
      (fst,snd)


  (*******************************************************)
  (* Parallel Prefix Sum                                 *)
  (*******************************************************)

  (* Here you will implement a version of the parallel prefix scan for a sequence 
   * [a0; a1; ...], the result of scan will be [f base a0; f (f base a0) a1; ...] *)
  let scan (f: 'a -> 'a -> 'a) (base: 'a) (seq: 'a t) : 'a t =
    failwith "implement me"
        
end







