// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Caching.Distributed;
using Google.Protobuf;
using System.Diagnostics;
using OpenTelemetry.Trace;
using StackExchange.Redis;


namespace cartservice.cartstore
{
    public class RedisCartStore : ICartStore
    {
        private readonly IDistributedCache _cache;
        private static readonly ActivitySource ActivitySource = new("cartservice");


        public RedisCartStore(IDistributedCache cache)
        {
            _cache = cache;
        }

        public async Task AddItemAsync(string userId, string productId, int quantity)
        {
            Console.WriteLine($"AddItemAsync called with userId={userId}, productId={productId}, quantity={quantity}");

            using var activity = ActivitySource.StartActivity("RedisAddItem", ActivityKind.Client);
            activity?.SetTag("db.system", "redis");
            activity?.SetTag("db.operation", "SET");
            activity?.SetTag("db.redis.key", userId);
            activity?.SetTag("user.id", userId);
            activity?.SetTag("cart.item", productId);

            try
            {
                Hipstershop.Cart cart;
                var value = await _cache.GetAsync(userId);
                if (value == null)
                {
                    cart = new Hipstershop.Cart();
                    cart.UserId = userId;
                    cart.Items.Add(new Hipstershop.CartItem { ProductId = productId, Quantity = quantity });
                }
                else
                {
                    cart = Hipstershop.Cart.Parser.ParseFrom(value);
                    var existingItem = cart.Items.SingleOrDefault(i => i.ProductId == productId);
                    if (existingItem == null)
                    {
                        cart.Items.Add(new Hipstershop.CartItem { ProductId = productId, Quantity = quantity });
                    }
                    else
                    {
                        existingItem.Quantity += quantity;
                    }
                }
                await _cache.SetAsync(userId, cart.ToByteArray());
                activity?.SetStatus(ActivityStatusCode.Ok);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.AddEvent(new ActivityEvent("Exception", tags: new ActivityTagsCollection { { "exception.message", ex.Message } }));
                throw new Grpc.Core.RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.FailedPrecondition, ex.Message));
            }
        }

        public async Task EmptyCartAsync(string userId)
        {
            Console.WriteLine($"EmptyCartAsync called with userId={userId}");

            using var activity = ActivitySource.StartActivity("RedisEmptyCart", ActivityKind.Client);
            activity?.SetTag("db.system", "redis");
            activity?.SetTag("db.operation", "DELETE");
            activity?.SetTag("db.redis.key", userId);
            activity?.SetTag("user.id", userId);

            try
            {
                var cart = new Hipstershop.Cart();
                await _cache.SetAsync(userId, cart.ToByteArray());
                activity?.SetStatus(ActivityStatusCode.Ok);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.AddEvent(new ActivityEvent("Exception", tags: new ActivityTagsCollection { { "exception.message", ex.Message } }));
                throw new Grpc.Core.RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.FailedPrecondition, ex.Message));
            }
        }

        public async Task<Hipstershop.Cart> GetCartAsync(string userId)
        {
            Console.WriteLine($"GetCartAsync called with userId={userId}");

            using var activity = ActivitySource.StartActivity("RedisGetCart", ActivityKind.Client);
            activity?.SetTag("db.system", "redis");
            activity?.SetTag("db.operation", "GET");
            activity?.SetTag("db.redis.key", userId);
            activity?.SetTag("user.id", userId);

            try
            {
                // Access the cart from the cache
                var value = await _cache.GetAsync(userId);

                if (value != null)
                {
                    activity?.SetStatus(ActivityStatusCode.Ok);
                    return Hipstershop.Cart.Parser.ParseFrom(value);
                }

                // We decided to return empty cart in cases when user wasn't in the cache before
                activity?.SetStatus(ActivityStatusCode.Ok);
                return new Hipstershop.Cart();
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.AddEvent(new ActivityEvent("Exception", tags: new ActivityTagsCollection { { "exception.message", ex.Message } }));
                throw new Grpc.Core.RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.FailedPrecondition, ex.Message));
            }
        }

        public bool Ping()
        {
            try
            {
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
