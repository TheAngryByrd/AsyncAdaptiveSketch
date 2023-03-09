open System.Threading
open System.Threading.Tasks
open FSharp.Data.Adaptive
open System
open IcedTasks
type internal RefCountingTaskCreator<'a>(create : CancellationToken -> Task<'a>) =
    
    let mutable refCount = 0
    let mutable cache : option<Task<'a>> = None
    let mutable cancel : CancellationTokenSource = null
    
    member private x.RemoveRef() =
        lock x (fun () ->
            if refCount = 1 then
                refCount <- 0
                cancel.Cancel()
                cancel.Dispose()
                cancel <- null
                cache <- None
            else
                refCount <- refCount - 1
        )

    member x.New() =
        lock x (fun () ->
            match cache with
            | Some cache ->
                refCount <- refCount + 1
                CancelableTask(x.RemoveRef, cache)
            | None ->
                cancel <- new CancellationTokenSource()
                let task = create cancel.Token
                cache <- Some task
                refCount <- refCount + 1
                CancelableTask(x.RemoveRef, task)
        )
    

and [<Struct>] CancelableTask<'a>(cancel : unit -> unit, real : Task<'a>) =

    member x.GetAwaiter() = real.GetAwaiter()
    member x.Cancel() = 
        cancel()
    member x.Task =
        real

type asyncaval<'a> =
    inherit IAdaptiveObject
    abstract GetValue : AdaptiveToken -> CancelableTask<'a>
    
module AsyncAVal =
    
    type ConstantVal<'a>(value : Task<'a>) =
        inherit ConstantObject()
        
        interface asyncaval<'a> with
            member x.GetValue _ = CancelableTask(id, value)
        
    [<AbstractClass>]
    type AbstractVal<'a>() =
        inherit AdaptiveObject()
        abstract Compute : AdaptiveToken -> CancelableTask<'a>
        
        member x.GetValue token =
            x.EvaluateAlways token x.Compute
        
        interface asyncaval<'a> with
            member x.GetValue t = x.GetValue t
    
    let constant (value : 'a) =
        ConstantVal(Task.FromResult value) :> asyncaval<_>
     
    let ofTask (value : Task<'a>) =
        ConstantVal(value) :> asyncaval<_>
     
    let ofCancelableTask (value : CancelableTask<'a>) =
        ConstantVal(value.Task) :> asyncaval<_>
     
    let ofAVal (value : aval<'a>) =
        if value.IsConstant then
            ConstantVal(Task.FromResult (AVal.force value)) :> asyncaval<_>
        else
            { new AbstractVal<'a>() with
                member x.Compute t =
                    let real = Task.FromResult(value.GetValue t)
                    CancelableTask(id, real)
            } :> asyncaval<_>

    let map (mapping : 'a -> CancellationToken -> Task<'b>) (input : asyncaval<'a>) =
        let mutable cache : option<RefCountingTaskCreator<'b>> = None
        { new AbstractVal<'b>() with
            member x.Compute t =
                if x.OutOfDate || Option.isNone cache then
                    let ref =
                        RefCountingTaskCreator(cancellableTask {
                            let! ct = CancellableTask.getCancellationToken ()
                            let it = input.GetValue t
                            let s = ct.Register(fun () -> it.Cancel())
                            try
                                let! i = it
                                return! mapping i ct
                            finally
                                s.Dispose()
                            }    
                        )
                    cache <- Some ref
                    ref.New()
                else
                    cache.Value.New()
        } :> asyncaval<_>

    let map2 (mapping : 'a -> 'b -> CancellationToken -> Task<'c>) (ca : asyncaval<'a>) (cb : asyncaval<'b>) =
        let mutable cache : option<RefCountingTaskCreator<'c>> = None
        { new AbstractVal<'c>() with
            member x.Compute t =
                if x.OutOfDate || Option.isNone cache then
                    let ref =
                        RefCountingTaskCreator(cancellableTask {
                            let ta = ca.GetValue t
                            let tb = cb.GetValue t
                            
                            let! ct = CancellableTask.getCancellationToken ()
                            let s = ct.Register(fun () -> ta.Cancel(); tb.Cancel())

                            try
                                let! va = ta
                                let! vb = tb
                                return! mapping va vb ct
                            finally
                                s.Dispose()
                            }    
                        )
                    cache <- Some ref
                    ref.New()
                else
                    cache.Value.New()
        } :> asyncaval<_>

    // untested!!!!
    let bind (mapping : 'a -> CancellationToken -> asyncaval<'b>) (value : asyncaval<'a>) =
        let mutable cache : option<_> = None
        let mutable innerCache : option<_> = None
        let mutable inputChanged = 0
        let inners : ref<HashSet<asyncaval<'b>>> = ref HashSet.empty

        { new AbstractVal<'b>() with
            
            override x.InputChangedObject(_, o) =
                if System.Object.ReferenceEquals(o, value) then
                    inputChanged <- 1
                    lock inners (fun () ->
                        for i in inners.Value do i.Outputs.Remove x |> ignore
                        inners.Value <- HashSet.empty
                    )
            
            member x.Compute t =
                if x.OutOfDate then
                    if Interlocked.Exchange(&inputChanged, 0) = 1 || Option.isNone cache then
                        let outerTask =
                            RefCountingTaskCreator(cancellableTask {
                                let it = value.GetValue t
                                let! ct = CancellableTask.getCancellationToken ()
                                let s = ct.Register(fun () -> it.Cancel())

                                try
                                    let! i = it
                                    let inner = mapping i ct
                                    return inner
                                finally
                                    s.Dispose()
                                }
                            )
                        cache <- Some outerTask
                        
                    let outerTask = cache.Value
                    let ref = 
                        RefCountingTaskCreator(cancellableTask {
                            let innerCellTask = outerTask.New()
                            
                            let! ct = CancellableTask.getCancellationToken ()
                            let s = ct.Register(fun () -> innerCellTask.Cancel())

                            try
                                let! inner = innerCellTask
                                let innerTask = inner.GetValue t
                                lock inners (fun () -> inners.Value <- HashSet.add inner inners.Value)
                                let s2 =
                                    ct.Register(fun () ->
                                        innerTask.Cancel()
                                        lock inners (fun () -> inners.Value <- HashSet.remove inner inners.Value)
                                        inner.Outputs.Remove x |> ignore
                                    )
                                try
                                    let! innerValue = innerTask
                                    return innerValue
                                finally
                                    s2.Dispose()
                            finally
                                s.Dispose()
                            }    
                        )
                        
                    innerCache <- Some ref
                        
                    ref.New()
                else
                    innerCache.Value.New()
                    
        } :> asyncaval<_>


type AsyncAValBuilder () =
    
    member inline x.MergeSources(v1 : asyncaval<'T1>, v2 : asyncaval<'T2>) =
        AsyncAVal.map2 (fun a b _ -> 
               Task.FromResult(a, b)
        ) v1 v2
        
    // member inline x.MergeSources3(v1 : aval<'T1>, v2 : aval<'T2>, v3 : aval<'T3>) =
    //     AVal.map3 (fun a b c -> a,b,c) v1 v2 v3


    member inline x.BindReturn(value : asyncaval<'T1>, mapping: 'T1 -> CancellationToken -> Task<'T2>) =
        AsyncAVal.map mapping value

    member inline x.BindReturn(value : asyncaval<'T1>, mapping: 'T1 -> Task<'T2>) =
        AsyncAVal.map (fun data _ -> mapping data) value

    // member inline x.Bind2Return(v1 : aval<'T1>, v2 : aval<'T2>, mapping: 'T1 * 'T2 -> 'T3) =
    //     AVal.map2 (fun a b -> mapping(a,b)) v1 v2

    // member inline x.Bind3Return(v1 : aval<'T1>, v2: aval<'T2>, v3: aval<'T3>, mapping: 'T1 * 'T2 * 'T3 -> 'T4) =
    //     AVal.map3 (fun a b c -> mapping(a, b, c)) v1 v2 v3


    member inline x.Bind(value: asyncaval<'T1>, mapping: 'T1 -> CancellationToken -> asyncaval<'T2>) =
        AsyncAVal.bind (mapping) value

    member inline x.Bind(value: asyncaval<'T1>, mapping: 'T1  -> asyncaval<'T2>) =
        AsyncAVal.bind (fun data _ -> mapping data) value
        
    // member inline x.Bind2(v1: aval<'T1>, v2: aval<'T2>, mapping: 'T1 * 'T2 -> aval<'T3>) =
    //     AVal.bind2 (fun a b -> mapping(a,b)) v1 v2
        
    // member inline x.Bind3(v1: aval<'T1>, v2: aval<'T2>, v3: aval<'T3>, mapping: 'T1 * 'T2 * 'T3 -> aval<'T4>) =
    //     AVal.bind3 (fun a b c -> mapping(a, b, c)) v1 v2 v3

    member inline x.Return(value: 'T) =
        AsyncAVal.constant value

    member inline x.ReturnFrom(value: asyncaval<'T>) =
        value

    member inline x.Source(value : asyncaval<'T>) = value
[<AutoOpen>]
module AsyncAValBuilderExtensions =
    let asyncAVal = AsyncAValBuilder()
    type AsyncAValBuilder with
        member inline x.Source(value : aval<'T>) = AsyncAVal.ofAVal value
        member inline x.Source(value : Task<'T>) = AsyncAVal.ofTask value
        member inline x.Source(value : CancelableTask<'T>) = AsyncAVal.ofCancelableTask value

        member inline x.BindReturn(value : asyncaval<'T1>, mapping: 'T1 -> 'T2) =
            AsyncAVal.map (fun data _ -> mapping data |> Task.FromResult) value
            

[<EntryPoint>]
let main argv = 

    let returnValue: asyncaval<string> = asyncAVal {
        return "lol"
    }

    let returnsyncaval: asyncaval<string> = asyncAVal {
        return! asyncAVal {
            return "LOL"
        }
    }

    let returnaval: asyncaval<string> = asyncAVal {
        return! aval {
            return "LOL"
        }
    }

    let returnsTask: asyncaval<string> = asyncAVal {
        return! Task.FromResult("lol")
    }

    let returnsCancellableTask: asyncaval<string> = asyncAVal {
        return! CancelableTask(id, Task.FromResult("LOL"))
    }

    let mergeSourcesEx = asyncAVal {
        let! foo = returnValue
        and! bar = returnsyncaval
        and! baz = Task.FromResult("lol")

        return foo, bar, baz
    }

    let l = obj()
    let printfn fmt =
        fmt |> Printf.kprintf (fun str ->
            lock l (fun () ->
                System.Console.WriteLine str
            )
        )
    
    let input = cval 0

    let cts = new CancellationTokenSource()

    input.AddMarkingCallback(fun data -> 
        printfn $"input marked out of date: {data}"
    ) |> ignore
    input.AddCallback(fun data -> printfn $"input changed: {data}") |> ignore
    
    let rng = Random()

    let node1 =
        input |> AsyncAVal.ofAVal |> AsyncAVal.map (fun time ->
            cancellableTask {
                printfn "node1 start sleeping"
                try
                    let delayTime = rng.Next(500, 3000)
                    do! fun ct -> Task.Delay(delayTime, ct)
                    printfn "node1 done sleeping"
                    return time
                with e ->
                    printfn "node1 stop sleeping"
                    return raise e
            }
        )


    let evenNode =
        node1 |> AsyncAVal.map (fun time -> 
            cancellableTask {
                printfn "evenNode start sleeping"
                try
                    let delayTime = rng.Next(500, 3000)
                    do! fun ct -> Task.Delay(delayTime, ct)
                    printfn "evenNode done sleeping"
                    return $"{time} - even"
                with e ->
                    printfn "evenNode stop sleeping"
                    return raise e
            }
        )

        
    let oddNode =
        node1 |> AsyncAVal.map (fun time -> 
            cancellableTask {
                printfn "oddNode start sleeping"
                try
                    let delayTime = rng.Next(500, 3000)
                    do! fun ct -> Task.Delay(delayTime, ct)
                    printfn "oddNode done sleeping"
                    return $"{time} - odd"
                with e ->
                    printfn "oddNode stop sleeping"
                    return raise e
            }
        )
    
    let node3 = 
        asyncAVal {
            let! value =  node1
            if value % 2 = 0 then
                return! evenNode
            else
                return! oddNode
        }

        
    let producer = task {
        let mutable nextNumber = 0
        while true do
            try
                let delayTime = rng.Next(1000, 4000)
                do! Task.Delay(delayTime)
                transact <| fun () -> 
                    input.Value <- nextNumber
                    nextNumber <- nextNumber + 1
            with e -> printfn "Producer error %A" e
    }

    let consumer = task {
        while true do

            try
                // printfn "restarting computation"
                let result = node3.GetValue(AdaptiveToken.Top)
                
                use d = input.AddMarkingCallback(fun () ->
                    printfn "cancelling consumer"
                    result.Cancel()
                )
                // printfn "restarting computation2"
                let! result = result
                printfn $"got {result}"
                d.Dispose()
            with
            | :? OperationCanceledException as e -> eprintfn "cancelled %A" e
            | e ->
                eprintfn "consume hardfail %A" e
                raise e
    }

    Console.ReadLine() |> ignore

    // let sleeper =
    //     input |> AsyncAVal.ofAVal |> AsyncAVal.map (fun time ct ->
    //         printfn "start sleeping"
    //         task {
    //             try
    //                 do! Task.Delay(time, ct)
    //                 printfn "done sleeping"
    //                 return time
    //             with e ->
    //                 printfn "stop sleeping"
    //                 return raise e
    //         }
    //     )
        
    // let a =
    //     sleeper |> AsyncAVal.map (fun time ct ->
    //         printfn "start a"
    //         task {
    //             try
    //                 do! Task.Delay(100, ct)
    //                 printfn "a done"
    //                 return time
    //             with e ->
    //                 printfn "a stopped"
    //                 return raise e
    //         }
    //     )
    // let b =
    //     sleeper |> AsyncAVal.map (fun time ct ->
    //         printfn "start b"
    //         task {
    //             try
    //                 do! Task.Delay(300, ct)
    //                 printfn "b done"
    //                 return time
    //             with e ->
    //                 printfn "b stopped"
    //                 return raise e
    //         }
    //     )
        
    // let c =
    //     sleeper |> AsyncAVal.map (fun time ct ->
    //         printfn "start c"
    //         task {
    //             try
    //                 do! Task.Delay(200, ct)
    //                 printfn "c done"
    //                 return time
    //             with e ->
    //                 printfn "c stopped"
    //                 return raise e
    //         }
    //     )
        
    // let d = (a,b) ||> AsyncAVal.map2 (fun a b _ -> task { return a - b })
        
        
        
    
    // sleeper.GetValue(AdaptiveToken.Top).Task.Result |> printfn "result: %A"
    
    // a.GetValue(AdaptiveToken.Top).Task.Result |> printfn "a: %A"
    // b.GetValue(AdaptiveToken.Top).Task.Result |> printfn "b: %A"
    // c.GetValue(AdaptiveToken.Top).Task.Result |> printfn "c: %A"
    // d.GetValue(AdaptiveToken.Top).Task.Result |> printfn "d: %A"
    
    
    // printfn "change"
    // transact (fun () -> input.Value <- 500)
    
    // let tc = c.GetValue(AdaptiveToken.Top)
    // let td = d.GetValue(AdaptiveToken.Top)
    
    // tc.Cancel()
    // td.Task.Result |> printfn "d = %A"
    
    
    // printfn "eval tasks"
    // let va = a.GetValue(AdaptiveToken.Top)
    // let vb = b.GetValue(AdaptiveToken.Top)
    
    // printfn "cancel a"
    // va.Cancel()
    
    // printfn "wait b"
    // vb.Task.Result |> printfn "b: %A"
    
    // printfn "change"
    // transact (fun () -> input.Value <- 400)
    
    // printfn "eval tasks"
    // let va = a.GetValue(AdaptiveToken.Top)
    // let vb = b.GetValue(AdaptiveToken.Top)
    
    // Thread.Sleep 50
    // printfn "cancel a"
    // va.Cancel()
    
    // printfn "cancel b"
    // vb.Cancel()
    
    // try va.Task.Wait() with _ -> ()
    // try vb.Task.Wait() with _ -> ()
    
    // let tc = c.GetValue(AdaptiveToken.Top)
    // printfn "c: %A" tc.Task.Result
    
    0
