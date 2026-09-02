import { describe, expect, test } from 'bun:test';

import workmuxStatusExtension from '../resources/pi/extensions/workmux-status';

type Handler = (event: unknown, context: unknown) => Promise<void> | void;
type AssistantMessage = {
  role: 'assistant';
  stopReason: string;
  errorMessage?: string;
};

function createHarness(initialMessage: AssistantMessage) {
  const handlers = new Map<string, Handler>();
  const statuses: string[] = [];
  let branch = [{ type: 'message', message: initialMessage }];

  const pi = {
    exec: async (_command: string, args: string[]) => {
      if (args[0] === 'set-window-status') {
        statuses.push(args[1]);
      }
      return { stdout: '', stderr: '', code: 0, killed: false };
    },
    on: (name: string, handler: Handler) => handlers.set(name, handler),
  };
  workmuxStatusExtension(pi as never);

  return {
    statuses,
    setMessage(message: AssistantMessage) {
      branch = [...branch, { type: 'message', message }];
    },
    async emit(name: string) {
      await handlers.get(name)?.({}, {
        sessionManager: { getBranch: () => branch },
      });
    },
  };
}

const stoppedMessage = (): AssistantMessage => ({
  role: 'assistant',
  stopReason: 'stop',
});

describe('pi workmux status extension', () => {
  test('reports done only after the full agent run settles', async () => {
    const harness = createHarness(stoppedMessage());

    await harness.emit('agent_start');
    await harness.emit('agent_end');
    expect(harness.statuses).toEqual(['working']);

    await harness.emit('agent_settled');
    expect(harness.statuses).toEqual(['working', 'done']);
  });

  test.each([
    {
      role: 'assistant' as const,
      stopReason: 'aborted',
    },
    {
      role: 'assistant' as const,
      stopReason: 'error',
      errorMessage: 'The operation was aborted.',
    },
  ])('stays working after an aborted turn', async (message) => {
    const harness = createHarness(message);

    await harness.emit('agent_start');
    await harness.emit('agent_settled');

    expect(harness.statuses).toEqual(['working']);
  });

  test('reports done after the continuation completes', async () => {
    const harness = createHarness({
      role: 'assistant',
      stopReason: 'error',
      errorMessage: 'The operation was aborted.',
    });

    await harness.emit('agent_start');
    await harness.emit('agent_settled');
    harness.setMessage(stoppedMessage());
    await harness.emit('agent_start');
    await harness.emit('agent_settled');

    expect(harness.statuses).toEqual(['working', 'working', 'done']);
  });
});
