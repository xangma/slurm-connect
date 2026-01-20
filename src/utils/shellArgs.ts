export function isShellOperatorToken(token: string): boolean {
  return token === '&&' || token === '||' || token === ';' || token === '|' || token === '&';
}

export function quoteShellArg(value: string): string {
  if (value === '') {
    return "''";
  }
  if (/^[A-Za-z0-9_./:@%+=~\-]+$/.test(value)) {
    return value;
  }
  return "'" + value.replace(/'/g, "'\\''") + "'";
}

export function splitShellArgs(input: string): string[] {
  const result: string[] = [];
  let current = '';
  let inSingle = false;
  let inDouble = false;
  let escaping = false;

  const pushCurrent = () => {
    if (current.length > 0) {
      result.push(current);
      current = '';
    }
  };

  for (let i = 0; i < input.length; i += 1) {
    const ch = input[i];

    if (escaping) {
      current += ch;
      escaping = false;
      continue;
    }

    if (inSingle) {
      if (ch === "'") {
        inSingle = false;
        continue;
      }
      current += ch;
      continue;
    }

    if (inDouble) {
      if (ch === '"') {
        inDouble = false;
        continue;
      }
      if (ch === '\\') {
        escaping = true;
        continue;
      }
      current += ch;
      continue;
    }

    if (ch === '\\') {
      escaping = true;
      continue;
    }
    if (ch === "'") {
      inSingle = true;
      continue;
    }
    if (ch === '"') {
      inDouble = true;
      continue;
    }
    if (ch === ',' || /\s/.test(ch)) {
      pushCurrent();
      continue;
    }
    if (ch === '&' || ch === '|' || ch === ';') {
      pushCurrent();
      if ((ch === '&' || ch === '|') && input[i + 1] === ch) {
        result.push(ch + ch);
        i += 1;
      } else {
        result.push(ch);
      }
      continue;
    }
    current += ch;
  }

  pushCurrent();
  return result;
}

export function joinShellCommand(tokens: string[]): string {
  return tokens
    .filter((token) => token.length > 0)
    .map((token) => (isShellOperatorToken(token) ? token : quoteShellArg(token)))
    .join(' ');
}
