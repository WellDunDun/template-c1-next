import { NextRequest, NextResponse } from "next/server";
import { cookies } from 'next/headers';
import OpenAI from "openai";
import type { ChatCompletionMessageParam } from "openai/resources.mjs";
import { transformStream } from "@crayonai/stream";
import { getMessageStore } from "./messageStore";

export async function POST(req: NextRequest) {
  const { prompt, threadId, responseId } = (await req.json()) as {
    prompt: ChatCompletionMessageParam & { id: string };
    threadId: string;
    responseId: string;
  };

  console.log('Chat API called with:', { threadId, promptRole: prompt.role, responseId });
  console.log('THESYS_API_KEY present:', !!process.env.THESYS_API_KEY);

  // Try to extract auth token from multiple sources
  let authToken = '';
  
  // 1. Try from Authorization header
  const authHeader = req.headers.get('authorization');
  if (authHeader) {
    authToken = authHeader.replace('Bearer ', '');
    console.log('Auth token from header:', authToken ? 'Present' : 'Missing');
  }
  
  // 2. Try from cookies as fallback
  if (!authToken) {
    try {
      const cookieStore = await cookies();
      authToken = cookieStore.get('authToken')?.value || '';
      console.log('Auth token from cookies:', authToken ? 'Present' : 'Missing');
    } catch (error) {
      console.log('Could not read cookies:', error);
    }
  }
  
  // 3. Try from request body as last resort (if SDK passes it)
  if (!authToken && 'authToken' in req) {
    authToken = (req as { authToken?: string }).authToken || '';
    console.log('Auth token from request body:', authToken ? 'Present' : 'Missing');
  }

  const client = new OpenAI({
    baseURL: "https://api.thesys.dev/v1/embed",
    apiKey: process.env.THESYS_API_KEY,
  });

  const messageStore = getMessageStore(threadId, authToken);
  
  try {
    await messageStore.addMessage(prompt);
  } catch (error) {
    console.error('Error adding prompt message:', error);
    // Continue anyway - we'll try to get existing messages
  }

  // Get existing messages for context
  const existingMessages = await messageStore.getOpenAICompatibleMessageList();
  console.log('Retrieved existing messages:', existingMessages.length);

  const llmStream = await client.chat.completions.create({
    model: "c1-nightly",
    messages: existingMessages,
    stream: true,
  });

  // Unwrap the OpenAI stream to a C1 stream
  const responseStream = transformStream(
    llmStream,
    (chunk) => {
      return chunk.choices[0]?.delta?.content;
    },
    {
      onEnd: async ({ accumulated }) => {
        const message = accumulated.filter((chunk) => chunk).join("");
        try {
          await messageStore.addMessage({
            id: responseId,
            role: "assistant",
            content: message,
          });
          console.log('Assistant message stored successfully');
        } catch (error) {
          console.error('Error storing assistant message:', error);
          // Don't fail the response if message storage fails
        }
      },
    }
  ) as ReadableStream<string>;

  return new NextResponse(responseStream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  });
}
