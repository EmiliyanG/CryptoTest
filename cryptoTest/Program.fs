// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

//#r "System.IO.dll"
//#r "System.Security.dll"
open System
open System.IO
open System.Security.Cryptography


module Regex = 
    open System.Text.RegularExpressions
    open System

    [<Literal>] 
    let pathToFileRegex = """^(?:[\w]\:|\\)(\\[a-zA-Z_\-\s0-9\.\$\(\)]+)+\.(.+)$"""

    let (|ParseRegex|_|) regex str =
        let m = Regex(regex).Match(str)
        if m.Success then Some str
        else None
    
    let (|FilePath|_|) arg = 
        if String.Compare("--filePath", arg, StringComparison.OrdinalIgnoreCase) = 0
        then Some() else None


    let (|ValidPath|_|) arg = 
        match arg with
        | ParseRegex pathToFileRegex s -> Some s
        | _ -> None

    
module ParseCommandLineArgs = 
    open Regex
  
    type CommandLineOptions = {filePath: string option}

    let defaultOptions = {filePath = None}
    
    let rec internal parseCommandLine optionsSoFar args  = 
        match args with 
        // empty list means we're done.
        | [] -> optionsSoFar  

        // match filePath flag
        | FilePath::xs -> 
            match xs with 
            | ValidPath x ::xss -> 
                let newOptionsSoFar = { optionsSoFar with filePath=Some x}
                parseCommandLine newOptionsSoFar xss  
            | _ ->
                eprintfn "--filePath needs a second argument or the provided argument is not valid" 
                parseCommandLine optionsSoFar xs  

        // handle unrecognized option and keep looping
        | x::xs -> 
            eprintfn "Option '%s' is not recognized" x
            parseCommandLine optionsSoFar xs  
    
    let parseCommandLineArguments args =
        parseCommandLine defaultOptions args
    
    let printExceptedUsage() = 
        printfn "\nexpected usage: "
        printfn "--filePath <path-to-file>\<filename.extension>"


module Async =
    open System.Threading

    ///use a Semaphore to implement the concept of threads throttling. 
    ///At any point of time only a fixed number (equal to the available environment's processors) of threads are allowed to run.
    let inline ParallelWithThrottle millisecondsTimeout (threads:Async<'b> seq) =
        let threadsLimit = Environment.ProcessorCount
        let semaphore = new System.Threading.SemaphoreSlim(threadsLimit, threadsLimit)

        let mutable threadsCount = threads |> Seq.length 

        threads 
        |> Seq.map(
            fun thread -> 
                async {
                    
                    // try entering the semaphore 
                    let! isHandleAquired = 
                        semaphore.WaitAsync(millisecondsTimeout = millisecondsTimeout)
                        |> Async.AwaitTask

                    match isHandleAquired with
                    | true -> //allowed to enter the semaphore 
                        try
                            return! thread
                        finally 
                            //calculate the count of remaining threads waiting to enter the semaphore
                            //As the threadsCount will be shared between multiple threads decrement the count using atomic operation
                            let remainingThreadsCount = Interlocked.Decrement(ref threadsCount)
                            match remainingThreadsCount with
                            | 0 -> semaphore.Dispose()
                            | _ -> semaphore.Release() |> ignore
                            
                    | false -> //could not enter the semaphore within the specified timeout
                        return! failwith "Failled to aquire handle" 
                }
            )
        |> Async.Parallel 


///return fileStream for the specified filepath
///the fileStream should be disposed 
let getStream(filePath) = 
    new FileStream(filePath, FileMode.Open, FileAccess.Read)


let readBytesFromFile filePath offset count =
    async{
        use stream  = getStream(filePath)
        let a = Array.zeroCreate count
        stream.Position <- offset
        stream.Read(a, 0, count) |> ignore
        stream.Dispose()
        stream.Close()
        return offset, MD5Cng.Create().ComputeHash(a)
    }


let rec generateListOfAsyncTasks filePath (length:int64) count offset asyncList = 
    match length with
    | l when l > int64(offset) -> 
        (readBytesFromFile filePath offset count)::asyncList
        |> generateListOfAsyncTasks filePath length count (offset+int64(count))
    |_ -> asyncList



let processFile filePath = 
    use stream = getStream(filePath)
    let length = stream.Length
    stream.Dispose()
    stream.Close()

    let x = 
        generateListOfAsyncTasks filePath length 1000000 0L []
        |> Async.ParallelWithThrottle 600000 //current timeout - 6 minutes
        |> Async.RunSynchronously
        |> Array.sortBy (fun (n,_) -> n)
        |> Array.map (fun (_,s) -> s)
        |> Array.concat
    
    
    printfn "%A" (MD5.Create().ComputeHash(x))

open ParseCommandLineArgs

[<EntryPoint>]
let main argv = 

    let a = 
        argv
        |> Array.toList
        |> parseCommandLineArguments

    match a.filePath with
    | Some path -> 
        let print = 
            printfn "%A > %s" (System.DateTime.Now)

        print ("Started processing file \"" + path + "\"") 
        processFile path
        print "finished"

    | None -> 
        printExceptedUsage()
        
    
    0 // return an integer exit code
