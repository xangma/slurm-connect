function uniqueList(values: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    if (!seen.has(value)) {
      seen.add(value);
      result.push(value);
    }
  }
  return result;
}

export function sanitizeModuleOutput(output: string): string {
  const withoutOsc = output.replace(/\u001B\][^\u0007]*(?:\u0007|\u001B\\)/g, '');
  const withoutAnsi = withoutOsc.replace(
    /[\u001B\u009B][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nq-uy=><]/g,
    ''
  );
  return withoutAnsi.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F\u0080-\u009F]/g, '');
}

export function parseModulesOutput(output: string): string[] {
  if (!output) {
    return [];
  }

  const sanitized = sanitizeModuleOutput(output);
  const lowered = sanitized.toLowerCase();
  if (lowered.includes('command not found') || lowered.includes('module: not found')) {
    return [];
  }

  const lines = sanitized.split(/\r?\n/);
  const entries: string[] = [];
  const isSeparatorToken = (token: string): boolean => /^-+$/.test(token);
  const isLegendLine = (line: string): boolean => {
    const lower = line.toLowerCase();
    if (lower.startsWith('where:')) {
      return true;
    }
    if (lower.includes('module is loaded') || lower.includes('module is auto-loaded')) {
      return true;
    }
    if (lower.includes('module is inactive') || lower.includes('module is hidden')) {
      return true;
    }
    if (lower.includes('module spider') || lower.includes('module help')) {
      return true;
    }
    if (lower.startsWith('to get ') || lower.startsWith('to find ') || lower.startsWith('to list ')) {
      return true;
    }
    if (lower.startsWith('use "module') || lower.startsWith('use module')) {
      return true;
    }
    return false;
  };
  const stripTrailingTag = (value: string): string => {
    let trimmed = value.trim();
    if (!trimmed) {
      return trimmed;
    }

    const defaultMatch = trimmed.match(/^(.*?)(?:\s*[<(]\s*(?:default|d)\s*[>)]\s*)$/i);
    if (defaultMatch) {
      const base = defaultMatch[1].trim();
      return base ? `${base} (default)` : trimmed;
    }

    const shortTagPattern = /^(.*?)(?:\s*[<(]\s*[a-zA-Z]{1,3}\s*[>)]\s*)$/;
    while (shortTagPattern.test(trimmed)) {
      trimmed = trimmed.replace(shortTagPattern, '$1').trim();
    }
    return trimmed;
  };
  const normalizeHeader = (value: string): string => {
    const trimmed = value.trim();
    if (!trimmed) {
      return '';
    }
    return trimmed.endsWith(':') ? trimmed : `${trimmed}:`;
  };
  const headerFromDashLine = (line: string): string | null => {
    const match = line.match(/^-+\s*(\/\S.*?)\s*-+$/);
    return match ? match[1] : null;
  };

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || isLegendLine(trimmed)) {
      continue;
    }

    if (trimmed.startsWith('/') && trimmed.endsWith(':')) {
      entries.push(trimmed);
      continue;
    }

    const dashedHeader = headerFromDashLine(trimmed);
    if (dashedHeader) {
      const normalized = normalizeHeader(dashedHeader);
      if (normalized) {
        entries.push(normalized);
      }
      continue;
    }

    const tokens = /\s/.test(trimmed)
      ? trimmed.split(/\s+/).map((value) => value.trim()).filter(Boolean)
      : [trimmed];
    for (const token of tokens) {
      if (isSeparatorToken(token)) {
        continue;
      }
      if (token.startsWith('/') && token.endsWith(':')) {
        entries.push(token);
        continue;
      }
      const cleaned = stripTrailingTag(token);
      if (cleaned) {
        entries.push(cleaned);
      }
    }
  }

  return uniqueList(entries);
}
