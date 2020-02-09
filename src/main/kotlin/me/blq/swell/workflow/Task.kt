package me.blq.swell.workflow

import kotlin.reflect.KFunction

class Task (
    val function: KFunction<Unit>,
    vararg val args: Any
) {
    fun invoke() {
        function::class.java.methods[0].invoke(function, *args)
    }
}
