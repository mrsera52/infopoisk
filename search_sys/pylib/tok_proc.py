import os
import subprocess
from typing import List




class Stemmer:

    def __init__(self, binary_path: str):
        if not os.path.exists(binary_path):
            raise FileNotFoundError(f"Tokenizer binary not found: {binary_path}")
        self._proc = subprocess.Popen(
            [binary_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,

            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1,
        )

    def process(self, text: str) -> List[str]:
        if not text:
            return []
        clean = text.replace('\n', ' ')
        try:
            self._proc.stdin.write(clean + '\n')
            self._proc.stdin.flush()
        except BrokenPipeError:
            return []

        tokens: List[str] = []
        while True:
            line = self._proc.stdout.readline()
            if not line:
                break
            line = line.strip()
            if line == '__END_DOC__':
                break
            if line:
                tokens.append(line)
        return tokens

    def shutdown(self):
        if self._proc:
            self._proc.stdin.close()
            self._proc.stdout.close()
            self._proc.terminate()
            self._proc.wait()
