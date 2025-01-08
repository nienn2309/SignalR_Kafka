using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using System.Text.RegularExpressions;

namespace SignalR_Kafka.SignalR
{
    public class ChatHub : Hub
    {
        public async Task SendMessageToGroup(string conversationId, string user, string message)
        {
            var currentTime = DateTime.UtcNow;
            await Clients.Group(conversationId).SendAsync("ReceiveMessage", user, message, conversationId);
            await Clients.All.SendAsync("ConversationTimeUpdated", conversationId, currentTime);
        }

        public async Task JoinGroup(string conversationId)
        {
            Console.WriteLine($"{Context.ConnectionId}");
            await Groups.AddToGroupAsync(Context.ConnectionId, conversationId);
            Console.WriteLine($"User {Context.ConnectionId} joined group {conversationId}");
        }

        public async Task LeaveGroup(string conversationId)
        {
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, conversationId);
            Console.WriteLine($"User {Context.ConnectionId} left group {conversationId}");
        }

        public async Task NotifyNewConversation(string conversationId, string name)
        {
            await Clients.All.SendAsync("NewConversation", conversationId, name);
        }

        public override Task OnConnectedAsync()
        {
            Console.WriteLine($"User connected: {Context.ConnectionId}");
            return base.OnConnectedAsync();
        }

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            if (exception != null)
            {
                Console.WriteLine($"User disconnected with error: {exception.Message}");
            }
            else
            {
                Console.WriteLine("User disconnected");
            }
            return base.OnDisconnectedAsync(exception);
        }
    }
}
