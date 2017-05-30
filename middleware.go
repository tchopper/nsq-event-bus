package bus

type Middleware func(HandlerFunc) HandlerFunc

func chain(middlewares []Middleware, worker HandlerFunc) HandlerFunc {
	// return if the middlewares are blank
	if len(middlewares) == 0 {
		return worker
	}

	// Wrap the end handler with the middleware chain
	h := middlewares[len(middlewares)-1](worker)
	for i := len(middlewares) - 2; i >= 0; i-- {
		h = middlewares[i](h)
	}

	return h
}

func (el *EventListener) AddMiddleware(fn Middleware) {
	el.middleware = append(el.middleware, fn)
}
