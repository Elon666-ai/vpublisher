package tracer


func AsyncRunCoroutine(cb func()) {
	go func() {
		defer TryException()
		cb()
	}()
}