from dotenv import load_dotenv #allows us to import variables from the .env file
from typing import Annotated, List
from langgraph.graph import StateGraph, START, END #representation of graph for agent's decision flow with START and END node
from langgraph.graph.message import add_messages
from langchain.chat_models import init_chat_model #allows us to initialize an LLM in Langgraph
from typing_extensions import TypedDict #Typings we need in python
from pydantic import BaseModel, Field
from web_operations import serp_search, reddit_search_api, reddit_post_retrieval
from prompts import (get_reddit_analysis_messages, get_google_analysis_messages,
                    get_bing_analysis_messages, get_reddit_url_analysis_messages,
                    get_synthesis_messages)


load_dotenv() #looks for the presence of a .env file and loads those in so we can use it within our python code
llm = init_chat_model("gpt-4o") #passing in the model we want the agent to use


class State(TypedDict):
    #Messages that the user sends into the graph that itll process
    messages : Annotated[list, add_messages] #messages is of Type list and when we want to add new messages, we call "Add_messages" function
    user_question: str | None
    google_results: str | None
    bing_results: str | None
    reddit_results: str | None
    selected_reddit_urls: list[str] | None
    reddit_post_data: list | None #data for the selected URLs
    google_analysis: str | None #LLM analysis after we get our results
    reddit_analysis: str | None
    final_answer: str | None

#We define a python class and pass that to a LLM. We then tell the LLM that we need the output in a particular format
class RedditURLAnalysis(BaseModel):
    #Field informs us what we want the model to populate the field with
    selected_urls: List[str] = Field(description="List of Reddit URLs that contain valuable information for answering the user's question")
    #When we pass this to the LLM, well have a list of strings of the urls that we need



def google_search(state:State): #all of langgraphs nodes take in a state
    user_question = state.get("user_question", "")
    print(f"Searching Google for {user_question}")

    google_results = serp_search(user_question, engine="google")
    return {"google_results": google_results}

def bing_search(state:State):
    user_question = state.get("user_question", "")
    print(f"Searching Bing for {user_question}")
    bing_results = serp_search(user_question, engine="bing")
    return {"bing_results": bing_results}

def reddit_search(state:State):
    user_question = state.get("user_question", "")
    print(f"Searching Reddit for {user_question}")

    #Passing in users question to search on reddit
    reddit_results = reddit_search_api(user_question)
    print(reddit_results)
    return {"reddit_results": reddit_results}


def analyze_reddit_posts(state: State):
    user_question = state.get("user_question", "")
    reddit_results = state.get("reddit_results", "")
    #if theres no reddit urls, we return the selected urls which is an empty list
    if not reddit_results:
        return {"selected_reddit_urls": []}

#Pydantic models forces the llm to give the response in a particular format, which we define
    structured_llm = llm.with_structured_output(RedditURLAnalysis)
    #Generates the messages we need to  pass to llm (in this case its the system and user prompt)
    messages = get_reddit_url_analysis_messages(user_question, reddit_results)

    #Invoking the LLM with the provided messages and obtaining the selected urls
    try:
        analysis = structured_llm.invoke(messages)
        #Getting selected urls from response
        selected_urls = analysis.selected_urls

        print("Selected URLs:")
        for i, url in enumerate(selected_urls, 1):
            print(f"  {i}.  {url}")

    except Exception as e:
        print(e)
        selected_urls = []
    return {"selected_urls": selected_urls}


    return {"selected_reddit_urls": selected_urls}
#We need to retrieve reddit comments and then pass them to the llm
def retrieve_reddit_posts(state:State):
    print("Getting Reddit posts comments")
    #Getting selected urls from reddit post
    selected_urls = state.get("selected_urls", [])

    if not selected_urls:
        return {"reddit_post_data": []}

    print(f"Processing {len(selected_urls)} Reddit posts")

    #Grabs comment data from selected urls
    reddit_post_data = reddit_post_retrieval(selected_urls)

    if reddit_post_data:
        print(f"Successfully got {len(reddit_post_data)} posts")
    else:
        print("Failed to get post data")
        reddit_post_data = []

    print(reddit_post_data)
    return {"reddit_post_data": reddit_post_data}

def analyze_google_results(state:State):
    print("Analyzing Google Search results")

    user_question = state.get("user_question", "")
    google_results = state.get("google_results", "")

    messages = get_google_analysis_messages(user_question, google_results)

    reply = llm.invoke(messages)

    return {"google_analysis": reply.content}

def analyze_bing_results(state:State):
    print("Analyzing Bing Search results")

    user_question = state.get("user_question", "")
    bing_results = state.get("bing_results", "")

    messages = get_bing_analysis_messages(user_question, bing_results)

    reply = llm.invoke(messages)

    return {"bing_analysis": reply.content}

def analyze_reddit_results(state:State):
    print("Analyzing Reddit Search results")

    user_question = state.get("user_question", "")
    reddit_results = state.get("reddit_results", "")
    reddit_post_data = state.get("reddit_post_data", "")

    messages = get_reddit_analysis_messages(user_question, reddit_results, reddit_post_data)

    reply = llm.invoke(messages)

    return {"reddit_analysis": reply.content}


def synthesize_analyses(state:State):

    print("Combine all results together")
    user_question = state.get("user_question", "")
    google_analysis = state.get("google_analysis", "")
    bing_analysis = state.get("bing_analysis", "")
    reddit_analysis = state.get("reddit_analysis", "")

    #Get all our answers and synthesize them
    messages = get_synthesis_messages(user_question, google_analysis, bing_analysis)

    reply = llm.invoke(messages)
    final_answer = reply.content

    return {"final_answer": final_answer, "messages":[{"role": "assistant", "content": final_answer}]}

#Creating the connection between nodes, we always pass in our State
graph_builder = StateGraph(State)
graph_builder.add_edge(START, "google_search")
graph_builder.add_edge(START, "bing_search")
graph_builder.add_edge(START, "reddit_search")

#Adding nodes to graph via providing a name and pointing to its respective function

graph_builder.add_node("google_search", google_search)
graph_builder.add_node("bing_search", bing_search)
graph_builder.add_node("reddit_search", reddit_search)
graph_builder.add_node("analyze_reddit_posts", analyze_reddit_posts)
graph_builder.add_node("retrieve_reddit_posts", retrieve_reddit_posts)
graph_builder.add_node("analyze_google_results", analyze_google_results)
graph_builder.add_node("analyze_bing_results", analyze_bing_results)
graph_builder.add_node("analyze_reddit_results", analyze_reddit_results)
graph_builder.add_node("synthesize_analyses", synthesize_analyses)


#Connecting nodes to each other
graph_builder.add_edge("google_search", "analyze_reddit_posts")
graph_builder.add_edge("bing_search", "analyze_reddit_posts")
graph_builder.add_edge("reddit_search", "analyze_reddit_posts")
graph_builder.add_edge("analyze_reddit_posts", "retrieve_reddit_posts")

graph_builder.add_edge("retrieve_reddit_posts", "analyze_google_results")
graph_builder.add_edge("retrieve_reddit_posts", "analyze_bing_results")
graph_builder.add_edge("retrieve_reddit_posts", "analyze_reddit_results")

graph_builder.add_edge("analyze_google_results", "synthesize_analyses")
graph_builder.add_edge("analyze_bing_results", "synthesize_analyses")
graph_builder.add_edge("analyze_reddit_results", "synthesize_analyses")
graph_builder.add_edge("synthesize_analyses", END)

graph = graph_builder.compile() #Executes the graph so we can run it and pass in a message, effectively going through nodes, updating states, and then print out the state

def run_chatbot(): #allows us to execute our agent, so we can run through the graph
    print("Multi-Source Research Agent")
    print("Type 'exit' to quit\n")

    while True:
        user_input = input("Ask me anything: ")
        if user_input.lower() == "exit":
            print("Goodbye!")
            break

    # initializing a starting state if a user doesnt exit; this is the agent’s starting point or memory log
    state = {
        #The message is from the user and its (whatever user_input is)
        "messages": [{"role":"user", "content": user_input}],
        "user_question": user_input,
        "google_results": None,
        "bing_results": None,
        "reddit_results": None,
        "selected_reddit_urls": None,
        "reddit_post_data": None,
        "google_analysis": None,
        "reddit_analysis": None,
        "bing_analysis": None,
        "final_answer": None,
    }

    print("\n Starting parallel research process...")
    print("Launching Google, Bing, and Reddit searches...\n")
    print("-" * 80)

    #Invoking the agent and begin running
    final_state = graph.invoke(state)
    #if the final_state contains a final_answer, print it out
    if final_state.get("final_answer"):
        print(f"\nFinal Answer:\n{final_state.get('final_answer')}\n")

    print("-" * 80)

if __name__ == "__main__":
    run_chatbot()
