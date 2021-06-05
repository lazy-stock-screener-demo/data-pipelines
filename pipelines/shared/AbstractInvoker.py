class AbstractInvoker:
    def initiate_command(self):
        while len(self._command_queue_arr) > 0:
            try:
                command = self._command_queue_arr.pop(0)
                if command.is_abort_invoker():
                    break
                elif command.is_skip_command():
                    continue
                else:
                    command.execute()
            except Exception as err:
                print(err)
        return self._payload
